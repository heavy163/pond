"""QuantSupervise v2：截面盈利能力监控看板（不动旧看板）。

旧看板只有 4 个面板（BTC 价格、预测行数、两条净值），看不出"策略有没有吃到截面"。
新版围绕一个问题组织：

    当前截面的机会有多大？信号排序准不准？策略实盘吃到了多少？

分区：
  ① 截面机会  —— 截面收益离散度 / 分化带宽（市场给了多大空间）
  ② 信号效力  —— 每日 IC(1h/24h/72h)、IC 累计、10 分组超额、多空组超额
  ③ 策略实盘  —— 净值、每小时收益、持仓宽度、收益与离散度的相关
  ④ 汇总      —— 近 7 日关键指标

数据口径（全部实测，不改库）：
  - 预测：quant.futures_prediction，model='lgmb_train2y_hr72_sharpe'，value 越大越看多
  - 前瞻收益：quant.kline_futures_1h 按 open_time 对齐，close(t+h)/close(t)-1；
    未来 bar 不存在时置 nan（不能当 0 或 -1，否则会把"没数据"算成暴跌）
  - 超额收益：每小时先减去截面均值，再按分组平均，避免把大盘涨跌算进"选股能力"
  - 实盘：quant.future_wallet(strategy='neutral_v1' 多台机器) / quant.future_position

用法:
  /usr/bin/python3.12 ops/grafana/deploy_quant_supervise.py
  /usr/bin/python3.12 ops/grafana/deploy_quant_supervise.py --verify
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from pathlib import Path

GF = os.environ.get("GRAFANA_URL", "http://192.168.1.4:3000")
TOKEN = Path(os.environ.get("GRAFANA_TOKEN_FILE", "~/.hermes/secrets/grafana_token")).expanduser().read_text().strip()
DS_UID = "edvdze371o5q8f"
DS_TYPE = "grafana-clickhouse-datasource"
DASH_UID = "quant-supervise-v2"
MODEL = "lgmb_train2y_hr72_sharpe"
STRATEGY = "neutral_v1"


def api(path: str, method: str = "GET", body: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"{GF}{path}", method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        raw = r.read().decode()
        return json.loads(raw) if raw else {}


def q(raw_sql: str, fmt: int = 1) -> dict:
    return {"refId": "A", "datasource": {"type": DS_TYPE, "uid": DS_UID}, "rawSql": raw_sql,
            "queryType": "sql", "rawQuery": True, "format": fmt, "editorType": "sql"}


# ── 公共 CTE ─────────────────────────────────────────────────────
CTE = f"""
WITH pr AS (
    SELECT datetime AS t, code, value, value_group
    FROM quant.futures_prediction
    WHERE model = '{MODEL}' AND datetime >= $__fromTime AND datetime <= $__toTime
),
kl AS (
    SELECT code, open_time AS t, close
    FROM quant.kline_futures_1h
    WHERE open_time >= $__fromTime - INTERVAL 3 DAY
      AND open_time <= $__toTime + INTERVAL 3 DAY
      AND close > 0
),
fwd AS (
    SELECT a.code AS code, a.t AS t,
           if(b.close > 0, b.close / a.close - 1, nan) AS r1,
           if(c.close > 0, c.close / a.close - 1, nan) AS r24,
           if(d.close > 0, d.close / a.close - 1, nan) AS r72
    FROM kl a
    LEFT JOIN kl b ON a.code = b.code AND b.t = a.t + INTERVAL 1 HOUR
    LEFT JOIN kl c ON a.code = c.code AND c.t = a.t + INTERVAL 24 HOUR
    LEFT JOIN kl d ON a.code = d.code AND d.t = a.t + INTERVAL 72 HOUR
),
j AS (
    SELECT pr.t AS t, pr.value AS value, pr.value_group AS vg,
           fwd.r1 AS r1, fwd.r24 AS r24, fwd.r72 AS r72
    FROM pr INNER JOIN fwd ON fwd.code = pr.code AND fwd.t = pr.t
)
""".strip()

# ── 各面板 SQL ───────────────────────────────────────────────────
SQL_IC_DAILY = CTE + """
SELECT d, max(ic1) AS "IC_1h", max(ic24) AS "IC_24h", max(ic72) AS "IC_72h" FROM (
    SELECT toDate(t) AS d, rankCorr(value, r1) AS ic1, NULL AS ic24, NULL AS ic72
    FROM j WHERE isFinite(r1) GROUP BY d
    UNION ALL
    SELECT toDate(t) AS d, NULL, rankCorr(value, r24), NULL
    FROM j WHERE isFinite(r24) GROUP BY d
    UNION ALL
    SELECT toDate(t) AS d, NULL, NULL, rankCorr(value, r72)
    FROM j WHERE isFinite(r72) GROUP BY d
) GROUP BY d ORDER BY d"""

SQL_IC_CUM = CTE + """
SELECT d, round(sumDaily, 4) AS "每日IC(72h)",
       round(runningAcc, 4) AS "累计IC(72h)" FROM (
    SELECT d, sumDaily,
           sum(sumDaily) OVER (ORDER BY d) AS runningAcc
    FROM (
        SELECT toDate(t) AS d, rankCorr(value, r72) AS sumDaily
        FROM j WHERE isFinite(r72) GROUP BY d
    )
) ORDER BY d"""

SQL_DECILE = CTE + """
SELECT g AS `分组`, round(avg(excess) * 1e4, 1) AS "未来24h超额(bp)", count() AS n
FROM (
    SELECT r24 - avg(r24) OVER (PARTITION BY t) AS excess,
           ntile(10) OVER (PARTITION BY t ORDER BY value) AS g
    FROM j WHERE isFinite(r24)
) GROUP BY g ORDER BY g"""

SQL_LS_DAILY = CTE + """
SELECT d,
       round(avgIf(excess, vg IN (9, 10)) * 1e4, 1) AS "多头组超额(bp)",
       round(avgIf(excess, vg IN (1, 2)) * 1e4, 1) AS "空头组超额(bp)",
       round((avgIf(excess, vg IN (9, 10)) - avgIf(excess, vg IN (1, 2))) * 1e4, 1) AS "多空价差(bp)"
FROM (
    SELECT toDate(t) AS d, vg, r24 - avg(r24) OVER (PARTITION BY t) AS excess
    FROM j WHERE isFinite(r24)
) GROUP BY d ORDER BY d"""

SQL_DISPERSION = CTE + """
SELECT t AS time,
       round(stddevPop(r1) * 1e4, 1) AS "截面离散度(bp)",
       round((quantile(0.9)(r1) - quantile(0.1)(r1)) * 1e4, 1) AS "p90-p10带宽(bp)"
FROM j WHERE isFinite(r1) GROUP BY t ORDER BY t"""

SQL_DISPERSION_DAILY = CTE + """
SELECT toStartOfDay(t) AS time, round(avg(sd) * 1e4, 1) AS "日均离散度(bp)"
FROM (SELECT t, stddevPop(r1) AS sd FROM j WHERE isFinite(r1) GROUP BY t)
GROUP BY toStartOfDay(t) ORDER BY time"""

# 三台机器的本金差一个数量级（约 2.7k / 4.2k / 25k），直接画原值小账户会被压成直线。
# 归一化到窗口起点 = 100，才是可比口径；绝对规模另开一个合计面板。
# ⚠️ 多实体不能靠「一个字符串标签列」：ClickHouse 插件不会按标签拆 frame，
# 983 行会被画成一条线（三台机器的值交错），看起来就是上蹿下跳。
# 必须把每台机器透视成独立数值列，才对得上时间轴。
SQL_WALLET = f"""
WITH w AS (
    SELECT machine, toStartOfHour(datetime) AS h, argMax(value, datetime) AS v
    FROM quant.future_wallet
    WHERE strategy = '{STRATEGY}' AND datetime >= $__fromTime AND datetime <= $__toTime
    GROUP BY machine, h
),
b AS (SELECT machine, argMin(v, h) AS base FROM w GROUP BY machine),
grid AS (
    SELECT hours.h AS h, machines.machine AS machine
    FROM (SELECT DISTINCT h FROM w) AS hours
    CROSS JOIN (SELECT DISTINCT machine FROM w) AS machines
)
SELECT f.h AS time,
       round(maxIf(f.v / f.base * 100, f.machine LIKE '%43.153.152.139%'), 2) AS "43.153 (30k)",
       round(maxIf(f.v / f.base * 100, f.machine LIKE '%43.133.207.73%'), 2) AS "43.133 (4.5k)",
       round(maxIf(f.v / f.base * 100, f.machine LIKE '%43.167.224.202%'), 2) AS "43.167 (3k)"
FROM (
    SELECT g.h AS h, g.machine AS machine, w2.v AS v, b.base AS base
    FROM grid AS g
    ASOF LEFT JOIN w AS w2 ON g.machine = w2.machine AND g.h >= w2.h
    INNER JOIN b ON b.machine = g.machine
) AS f
GROUP BY f.h ORDER BY time"""

# 三台机器不是同一分钟上报（xx:00:57 / xx:01:01 / xx:01:07），按小时加总会在
# 「某台还没报」时算成净值少了 4.5k。用 ASOF JOIN 前向填充到各自最后已知值。
SQL_WALLET_TOTAL = f"""
WITH w AS (
    SELECT machine, toStartOfHour(datetime) AS h, argMax(value, datetime) AS v
    FROM quant.future_wallet
    WHERE strategy = '{STRATEGY}' AND datetime >= $__fromTime AND datetime <= $__toTime
    GROUP BY machine, h
)
SELECT g.h AS time, round(sum(w2.v), 2) AS "合计净值(USDT)"
FROM (SELECT hours.h AS h, machines.machine AS machine
      FROM (SELECT DISTINCT h FROM w) AS hours
      CROSS JOIN (SELECT DISTINCT machine FROM w) AS machines) AS g
ASOF LEFT JOIN w AS w2 ON g.machine = w2.machine AND g.h >= w2.h
GROUP BY g.h ORDER BY time"""

SQL_WALLET_STATS = f"""
WITH w AS (
    SELECT machine, datetime AS time, value
    FROM quant.future_wallet
    WHERE strategy = '{STRATEGY}' AND datetime >= $__fromTime AND datetime <= $__toTime
),
d AS (
    SELECT machine, time, value,
           max(value) OVER (PARTITION BY machine ORDER BY time
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peak
    FROM w
)
SELECT machine AS `机器`,
       round(argMin(value, time), 2) AS `起点`,
       round(argMax(value, time), 2) AS `末值`,
       round((argMax(value, time) / argMin(value, time) - 1) * 100, 2) AS `区间收益%`,
       round(min(value / peak - 1) * 100, 2) AS `最大回撤%`,
       count() AS `快照数`
FROM d GROUP BY machine ORDER BY `区间收益%` DESC"""

SQL_PORT_RET = f"""
WITH w AS (
    SELECT machine, toStartOfHour(datetime) AS h, argMax(value, datetime) AS v
    FROM quant.future_wallet
    WHERE strategy = '{STRATEGY}' AND datetime >= $__fromTime AND datetime <= $__toTime
    GROUP BY machine, h
),
r AS (
    SELECT machine, h, v,
           -- |小时收益|>5% 视为出入金（value 是权益，转账会整块跳变），置 0 而不是丢弃该小时
           if(abs(v / lagInFrame(v) OVER (PARTITION BY machine ORDER BY h) - 1) > 0.05, 0,
              v / lagInFrame(v) OVER (PARTITION BY machine ORDER BY h) - 1) AS ret
    FROM w
)
SELECT h AS time, round(sum(ret * v) / sum(v) * 1e4, 1) AS "组合每小时收益(bp)"
FROM r GROUP BY h HAVING isFinite(sum(ret * v)) ORDER BY time"""

# future_position 的两个坑：
#   ① 不是逐小时全量快照 —— 完整快照每天 00:0x 写一次（约 1,550 行），其余是单条止损/平仓事件
#   ② 每天只写**一个子策略**（strategy_id 0/1/2 轮转），当天快照只有 1/3 的账本
# 所以持仓宽度、gross 都必须：只认完整快照（nrows>=100）+ 按 strategy_id 做 ASOF 前向填充后加总。
COHORT_SNAP = """
snap AS (
    SELECT toStartOfHour(datetime) AS h,
           strategy_id,
           count() AS nrows,
           sumIf(abs(stake_amount), side = 'long') + sumIf(abs(stake_amount), side = 'short') AS gross,
           uniqExactIf(pair, side = 'long'  AND status IN ('long', 'signal_exiting')) AS lcnt,
           uniqExactIf(pair, side = 'short' AND status IN ('short', 'signal_exiting')) AS scnt
    FROM quant.future_position
    WHERE datetime >= $__fromTime - INTERVAL 5 DAY AND datetime <= $__toTime
    GROUP BY h, strategy_id
    HAVING nrows >= 100
),
sgrid AS (
    SELECT hours.h AS h, ids.strategy_id AS strategy_id
    FROM (SELECT DISTINCT h FROM snap) AS hours
    CROSS JOIN (SELECT DISTINCT strategy_id FROM snap) AS ids
),
filled AS (
    SELECT g.h AS h, g.strategy_id AS strategy_id,
           s.gross AS gross, s.lcnt AS lcnt, s.scnt AS scnt
    FROM sgrid AS g
    ASOF LEFT JOIN snap AS s ON s.strategy_id = g.strategy_id AND g.h >= s.h
)
"""

WALLET_HOURLY = """
w AS (
    SELECT machine, toStartOfHour(datetime) AS h, argMax(value, datetime) AS v
    FROM quant.future_wallet
    WHERE strategy = 'neutral_v1' AND datetime >= $__fromTime AND datetime <= $__toTime
    GROUP BY machine, h
),
wgrid AS (
    SELECT hours.h AS h, ms.machine AS machine
    FROM (SELECT DISTINCT h FROM w) AS hours
    CROSS JOIN (SELECT DISTINCT machine FROM w) AS ms
),
wfilled AS (
    SELECT g.h AS h, s.v AS v
    FROM wgrid AS g
    ASOF LEFT JOIN w AS s ON s.machine = g.machine AND g.h >= s.h
)
"""

SQL_POS_WIDTH = "WITH " + COHORT_SNAP + """
SELECT h AS time,
       sum(lcnt) AS "多头品种数(3 cohort)",
       sum(scnt) AS "空头品种数(3 cohort)"
FROM filled GROUP BY h ORDER BY time"""

SQL_COHORT_GROSS = "WITH " + COHORT_SNAP + """
SELECT h AS time,
       round(maxIf(gross, strategy_id = 0), 1) AS "cohort0",
       round(maxIf(gross, strategy_id = 1), 1) AS "cohort1",
       round(maxIf(gross, strategy_id = 2), 1) AS "cohort2",
       round(sum(gross), 1) AS "合计gross"
FROM filled GROUP BY h ORDER BY time"""

SQL_EXPO = "WITH " + COHORT_SNAP + "," + WALLET_HOURLY + """
,
gross_h AS (SELECT h, sum(gross) AS g FROM filled GROUP BY h),
eq_h AS (SELECT h, sum(v) AS eq FROM wfilled GROUP BY h)
SELECT gross_h.h AS time,
       round(gross_h.g, 1) AS "gross(USDT)",
       round(eq_h.eq, 1) AS "权益(USDT)",
       round(gross_h.g / eq_h.eq * 100, 1) AS "仓位比例%"
FROM gross_h INNER JOIN eq_h ON eq_h.h = gross_h.h
ORDER BY time"""

# 单个标量：取最后一小时；三个 stat 各自输出一个数，不混列
SQL_STAT_EXPO_RATIO = (
    "WITH " + COHORT_SNAP + "," + WALLET_HOURLY + """
SELECT round(argMax(g, h) / argMax(eq, h) * 100, 1)
FROM (
    SELECT g.h AS h, g.g AS g, e.eq AS eq
    FROM (SELECT h, sum(gross) AS g FROM filled GROUP BY h) AS g
    INNER JOIN (SELECT h, sum(v) AS eq FROM wfilled GROUP BY h) AS e ON e.h = g.h
)"""
)

SQL_STAT_GROSS = (
    "WITH " + COHORT_SNAP + """
SELECT round(argMax(g, h), 0) FROM (SELECT h, sum(gross) AS g FROM filled GROUP BY h)"""
)

SQL_STAT_EQUITY = (
    "WITH " + WALLET_HOURLY + """
SELECT round(argMax(eq, h), 0) FROM (SELECT h, sum(v) AS eq FROM wfilled GROUP BY h)"""
)

SQL_STAT_COHORT = (
    "WITH " + COHORT_SNAP + """
SELECT round(avg(g), 0) FROM (
    SELECT strategy_id, argMax(gross, h) AS g FROM filled GROUP BY strategy_id
)"""
)

# 策略每小时收益 × 截面离散度（双轴）
SQL_RET_VS_DISP = CTE + f""",
w AS (
    SELECT machine, toStartOfHour(datetime) AS h, argMax(value, datetime) AS v
    FROM quant.future_wallet
    WHERE strategy = '{STRATEGY}' AND datetime >= $__fromTime AND datetime <= $__toTime
    GROUP BY machine, h
),
r AS (
    SELECT h, v,
           if(abs(v / lagInFrame(v) OVER (PARTITION BY machine ORDER BY h) - 1) > 0.05, 0,
              v / lagInFrame(v) OVER (PARTITION BY machine ORDER BY h) - 1) AS ret
    FROM w
),
port AS (
    SELECT h, sum(ret * v) / sum(v) AS p_ret FROM r GROUP BY h HAVING isFinite(p_ret)
),
disp AS (
    SELECT t, stddevPop(r1) AS sd FROM j WHERE isFinite(r1) GROUP BY t
)
SELECT port.h AS time,
       round(port.p_ret * 1e4, 1) AS "策略收益(bp)",
       round(disp.sd * 1e4, 1) AS "截面离散度(bp)"
FROM port INNER JOIN disp ON disp.t = port.h
ORDER BY time"""

# ── 统计量（stat 面板用单值）──
SQL_STAT_IC72 = CTE + """
SELECT round(avg(ic), 4) FROM (
    SELECT toDate(t) AS d, rankCorr(value, r72) AS ic FROM j WHERE isFinite(r72) GROUP BY d
)"""

SQL_STAT_ICIR = CTE + """
SELECT round(avg(ic) / nullIf(stddevPop(ic), 0), 3) FROM (
    SELECT toDate(t) AS d, rankCorr(value, r72) AS ic FROM j WHERE isFinite(r72) GROUP BY d
)"""

SQL_STAT_LS = CTE + """
SELECT round(avg(ls) * 1e4, 1) FROM (
    SELECT toDate(t) AS d,
           avgIf(e, vg IN (9, 10)) - avgIf(e, vg IN (1, 2)) AS ls
    FROM (SELECT t, vg, r24 - avg(r24) OVER (PARTITION BY t) AS e FROM j WHERE isFinite(r24))
    GROUP BY d
)"""

SQL_STAT_DISP = CTE + """
SELECT round(stddevPop(r1) * 1e4, 1) FROM j
WHERE isFinite(r1) AND t = (SELECT max(t) FROM j WHERE isFinite(r1))"""

SQL_STAT_RET7 = f"""
SELECT round((argMax(value, datetime) / argMin(value, datetime) - 1) * 100, 2)
FROM quant.future_wallet
WHERE strategy = '{STRATEGY}' AND datetime >= now() - INTERVAL 7 DAY
  AND machine = 'http://43.153.152.139:5918'"""

SQL_STAT_CORR = CTE + f""",
w AS (
    SELECT machine, toStartOfHour(datetime) AS h, argMax(value, datetime) AS v
    FROM quant.future_wallet
    WHERE strategy = '{STRATEGY}' AND datetime >= $__fromTime AND datetime <= $__toTime
    GROUP BY machine, h
),
r AS (SELECT h, v, if(abs(v / lagInFrame(v) OVER (PARTITION BY machine ORDER BY h) - 1) > 0.05, 0,
              v / lagInFrame(v) OVER (PARTITION BY machine ORDER BY h) - 1) AS ret FROM w),
port AS (SELECT h, sum(ret * v) / sum(v) AS p_ret FROM r GROUP BY h HAVING isFinite(p_ret)),
disp AS (SELECT t, stddevPop(r1) AS sd FROM j WHERE isFinite(r1) GROUP BY t)
SELECT round(corr(port.p_ret, disp.sd), 3)
FROM port INNER JOIN disp ON disp.t = port.h"""

# 汇总表（近 7 日）
SQL_SUMMARY = CTE + """
SELECT 'IC 1h（日均）' AS `指标`, round(avg(c), 4) AS `值` FROM (
    SELECT toDate(t) d, rankCorr(value, r1) c FROM j WHERE isFinite(r1) GROUP BY d)
UNION ALL
SELECT 'IC 24h（日均）', round(avg(c), 4) FROM (
    SELECT toDate(t) d, rankCorr(value, r24) c FROM j WHERE isFinite(r24) GROUP BY d)
UNION ALL
SELECT 'IC 72h（日均）', round(avg(c), 4) FROM (
    SELECT toDate(t) d, rankCorr(value, r72) c FROM j WHERE isFinite(r72) GROUP BY d)
UNION ALL
SELECT '多空价差 24h（日均 bp）', round(avg(c) * 1e4, 1) FROM (
    SELECT toDate(t) d, avgIf(e, vg IN (9,10)) - avgIf(e, vg IN (1,2)) c
    FROM (SELECT t, vg, r24 - avg(r24) OVER (PARTITION BY t) e FROM j WHERE isFinite(r24)) GROUP BY d)
UNION ALL
SELECT '截面离散度（日均 bp）', round(avg(s) * 1e4, 1) FROM (
    SELECT t, stddevPop(r1) s FROM j WHERE isFinite(r1) GROUP BY t)"""

# ── 市场状态与风格（2026-11-03 新增）──────────────────────────────
# 口径（全部实测过，不是估算）：
#   * universe = 模型覆盖的合约（futures_prediction 近 3 日出现过的 code）。
#     ⚠️ 不含 BTCUSDT：它在因子表里 51,183 行 / 5 年 全 NaN、预测表 0 行，
#        是 beta_tobtc 的基准而不是可交易标的。混进来会污染等权指数。
#   * 市场收益 = 每小时同权截面均值，再按日求和 → 等权指数日收益。
#   * 趋势效率 ER = |20日累计收益| / 20日累计绝对收益。0=纯震荡，1=单边。
#   * 波动 = 等权日收益 20 日 stddev × sqrt(365)（crypto 7×24）。
#   * 宽度 = 站上 120 小时均线的合约占比（取 20 日均值）。
#   阈值取近 1 年分位：ER p70=0.305、波动 p70=54.2%。用绝对阈值而非滑动分位，
#   面板更快，也不同为窗口变化而"改口径"。
UNI_MODEL = f"""(SELECT DISTINCT code FROM quant.futures_prediction
                WHERE model='{MODEL}' AND datetime >= $__fromTime - INTERVAL 3 DAY)"""

STATE_CTE = f"""
WITH kl AS (
  SELECT code, open_time t, close,
         close / lagInFrame(close) OVER (PARTITION BY code ORDER BY open_time) - 1 AS r,
         avg(close) OVER (PARTITION BY code ORDER BY open_time ROWS 119 PRECEDING) AS ma120,
         count() OVER (PARTITION BY code ORDER BY open_time ROWS 119 PRECEDING) AS c120
  FROM quant.kline_futures_1h
  WHERE open_time >= $__fromTime - INTERVAL 40 DAY AND open_time <= $__toTime
    AND close > 0 AND code IN {UNI_MODEL}
),
h AS (SELECT t, avgIf(r, isFinite(r)) mkt,
             countIf(c120 = 120 AND close > ma120) / nullIf(countIf(c120 = 120), 0) above
      FROM kl GROUP BY t),
d AS (SELECT toDate(t) d, sum(mkt) r, avg(above) ab FROM h WHERE isFinite(mkt) GROUP BY d),
s AS (SELECT d, r,
             abs(sum(r) OVER w) / nullIf(sum(abs(r)) OVER w, 0) er,
             stddevPop(r) OVER w * sqrt(365) vol,
             avg(ab) OVER w breadth,
             count() OVER w nw
      FROM d WINDOW w AS (ORDER BY d ROWS 19 PRECEDING))
"""

STATE_EXPR = ("multiIf(nw < 20, 0, er > 0.305 AND vol <= 0.542, 1, "
              "er > 0.305, 2, vol <= 0.542, 3, 4)")

SQL_STATE_SERIES = STATE_CTE + f"""
SELECT d AS time, {STATE_EXPR} AS "状态"
FROM s WHERE d >= toDate($__fromTime) ORDER BY time"""

SQL_STATE_TS = STATE_CTE + """
SELECT d AS time,
       round(er, 3) AS "趋势效率ER",
       round(vol * 100, 1) AS "年化波动%",
       round(breadth * 100, 1) AS "站上MA120%"
FROM s WHERE nw = 20 AND d >= toDate($__fromTime) ORDER BY time"""

SQL_STAT_STATE = STATE_CTE + f"""
SELECT {STATE_EXPR} AS "当前状态" FROM s WHERE nw = 20 ORDER BY d DESC LIMIT 1"""

SQL_STAT_ER = STATE_CTE + """
SELECT round(er, 3) AS "趋势效率ER" FROM s WHERE nw = 20 ORDER BY d DESC LIMIT 1"""

SQL_STAT_VOL = STATE_CTE + """
SELECT round(vol * 100, 1) AS "年化波动%" FROM s WHERE nw = 20 ORDER BY d DESC LIMIT 1"""

SQL_STAT_BREADTH = STATE_CTE + """
SELECT round(breadth * 100, 1) AS "站上MA120%" FROM s WHERE nw = 20 ORDER BY d DESC LIMIT 1"""

# 风格价差：每天 00:00（CST）一个截面，按因子分 5 组，Q5−Q1 的未来 24h 超额。
# 00:00 这个时点很关键：它让「前瞻 24h」正好等于策略按 toDate 记的那个自然日，
# 两份日收益才能同日对齐（用 08:00 会错开 8 小时，相关性会被稀释）。
STYLE_CTE = f"""
WITH f AS (
  SELECT jj_code code, close_time t,
         trad_mom_24 AS fmom, rv_24 AS fvol, amihud1_24 AS filliq,
         flowinratio_24 AS fflow, adx_24 AS fadx, rsi_24 AS frsi
  FROM quant.`1h_generated_v3_V3_0_0`
  WHERE close_time >= $__fromTime - INTERVAL 130 DAY AND close_time <= $__toTime
    AND toHour(close_time) = 0 AND isFinite(rv_24) AND isFinite(trad_mom_24)
    AND isFinite(amihud1_24) AND isFinite(flowinratio_24) AND isFinite(adx_24) AND isFinite(rsi_24)
),
kl AS (SELECT code, open_time t, close FROM quant.kline_futures_1h
       WHERE open_time >= $__fromTime - INTERVAL 132 DAY AND open_time <= $__toTime + INTERVAL 2 DAY
         AND close > 0),
fwd AS (SELECT a.code, a.t, if(b.close > 0, b.close / a.close - 1, nan) r24
        FROM kl a LEFT JOIN kl b ON b.code = a.code AND b.t = a.t + INTERVAL 24 HOUR),
j AS (SELECT f.t t, f.fmom, f.fvol, f.filliq, f.fflow, f.fadx, f.frsi, fwd.r24
      FROM f INNER JOIN fwd ON fwd.code = f.code AND fwd.t = f.t),
e AS (SELECT t, fmom, fvol, filliq, fflow, fadx, frsi,
             r24 - avg(r24) OVER (PARTITION BY t) ex
      FROM j WHERE isFinite(r24)),
g AS (SELECT toDate(t) d, ex,
             ntile(5) OVER (PARTITION BY t ORDER BY fmom) qm,
             ntile(5) OVER (PARTITION BY t ORDER BY fvol) qv,
             ntile(5) OVER (PARTITION BY t ORDER BY filliq) qi,
             ntile(5) OVER (PARTITION BY t ORDER BY fflow) qf,
             ntile(5) OVER (PARTITION BY t ORDER BY fadx) qa,
             ntile(5) OVER (PARTITION BY t ORDER BY -frsi) qr
      FROM e),
sp AS (SELECT d,
              (avgIf(ex, qm = 5) - avgIf(ex, qm = 1)) * 1e4 AS s_mom,
              (avgIf(ex, qv = 5) - avgIf(ex, qv = 1)) * 1e4 AS s_vol,
              (avgIf(ex, qi = 5) - avgIf(ex, qi = 1)) * 1e4 AS s_illiq,
              (avgIf(ex, qf = 5) - avgIf(ex, qf = 1)) * 1e4 AS s_flow,
              (avgIf(ex, qa = 5) - avgIf(ex, qa = 1)) * 1e4 AS s_adx,
              (avgIf(ex, qr = 5) - avgIf(ex, qr = 1)) * 1e4 AS s_rev
       FROM g GROUP BY d)
"""

SQL_STYLE_CUM = STYLE_CTE + """
SELECT d AS time,
       round(sum(s_mom) OVER (ORDER BY d), 1)   AS "动量",
       round(sum(s_vol) OVER (ORDER BY d), 1)   AS "高波动",
       round(sum(s_illiq) OVER (ORDER BY d), 1) AS "非流动性",
       round(sum(s_flow) OVER (ORDER BY d), 1)  AS "资金流",
       round(sum(s_adx) OVER (ORDER BY d), 1)   AS "趋势强度",
       round(sum(s_rev) OVER (ORDER BY d), 1)   AS "短期反转"
FROM sp WHERE d >= toDate($__fromTime) ORDER BY time"""

# 策略日收益（多机等权平均）——**必须剔出入金**，否则统计全是假的：
#   value 是权益，出入金会让它整千整万地跳（实测 2025-12-27 主账号 8,492→16,328、
#   2025-11-26 第二台 1,140→8,800、2026-07-13 主账号 12,720→22,993），
#   不处理时单日「收益」能到 +703%，把日均从 ~20bp 拉到 339bp。
#   做法：按小时取样→逐小时收益→|r|>5% 视为出入金置 0→日内连乘得日收益。
#   5% 的依据：该策略小时 |收益| 的 p99=1.81%、p99.9=28.6%，真实交易小时几乎不会越过 5%。
SRET_CTE = f"""
WITH h AS (SELECT machine, toStartOfHour(datetime) hh, argMax(value, datetime) v
           FROM quant.future_wallet
           WHERE strategy = '{STRATEGY}' AND datetime >= $__fromTime - INTERVAL 40 DAY
             AND datetime <= $__toTime
           GROUP BY machine, hh),
hr AS (SELECT machine, hh, v / lagInFrame(v) OVER (PARTITION BY machine ORDER BY hh) - 1 r FROM h),
hc AS (SELECT machine, hh, if(abs(r) > 0.05, 0, r) AS r FROM hr WHERE isFinite(r)),
dd AS (SELECT machine, toDate(hh) d, exp(sum(log(1 + r))) - 1 AS dr FROM hc GROUP BY machine, d),
sret AS (SELECT d, avg(dr) AS dret FROM dd GROUP BY d)
"""

SQL_STYLE_CORR = STYLE_CTE + "," + SRET_CTE.replace("WITH ", "") + """
SELECT '动量' AS "风格", round(corr(s_mom, dret), 3) AS "相关性",
       round(corr(s_mom, dret) * stddevSamp(dret) * 1e4 / nullIf(stddevSamp(s_mom), 0), 3) AS "Beta"
FROM sp INNER JOIN sret ON sp.d = sret.d
UNION ALL SELECT '高波动', round(corr(s_vol, dret), 3), round(corr(s_vol, dret) * stddevSamp(dret) * 1e4 / nullIf(stddevSamp(s_vol), 0), 3)
FROM sp INNER JOIN sret ON sp.d = sret.d
UNION ALL SELECT '非流动性', round(corr(s_illiq, dret), 3), round(corr(s_illiq, dret) * stddevSamp(dret) * 1e4 / nullIf(stddevSamp(s_illiq), 0), 3)
FROM sp INNER JOIN sret ON sp.d = sret.d
UNION ALL SELECT '资金流', round(corr(s_flow, dret), 3), round(corr(s_flow, dret) * stddevSamp(dret) * 1e4 / nullIf(stddevSamp(s_flow), 0), 3)
FROM sp INNER JOIN sret ON sp.d = sret.d
UNION ALL SELECT '趋势强度', round(corr(s_adx, dret), 3), round(corr(s_adx, dret) * stddevSamp(dret) * 1e4 / nullIf(stddevSamp(s_adx), 0), 3)
FROM sp INNER JOIN sret ON sp.d = sret.d
UNION ALL SELECT '短期反转', round(corr(s_rev, dret), 3), round(corr(s_rev, dret) * stddevSamp(dret) * 1e4 / nullIf(stddevSamp(s_rev), 0), 3)
FROM sp INNER JOIN sret ON sp.d = sret.d"""

# 策略在四种市场状态下的表现（这是「策略适应什么状态」的直接答案）
SQL_STATE_PERF = STATE_CTE + f""",
{SRET_CTE[5:]}
SELECT multiIf(any(st) = 1, '① 趋势·低波', any(st) = 2, '② 趋势·高波',
               any(st) = 3, '③ 震荡·低波', '④ 震荡·高波') AS "市场状态",
       count() AS "天数",
       round(avg(ret) * 1e4, 1) AS "策略日均bp",
       round(median(ret) * 1e4, 1) AS "中位bp",
       round(avg(ret) / nullIf(stddevPop(ret) / sqrt(count()), 0), 2) AS "t值",
       round(countIf(ret > 0) / count() * 100, 1) AS "胜率%",
       round(stddevPop(ret) * 1e4, 1) AS "日波动bp"
FROM (
    SELECT d, {STATE_EXPR} AS st, dret AS ret
    FROM s INNER JOIN sret ON s.d = sret.d WHERE nw = 20
) GROUP BY st ORDER BY st"""

# ── 布局 ─────────────────────────────────────────────────────────
P: list[dict] = []
_pid = 0


def nid() -> int:
    global _pid
    _pid += 1
    return _pid


def row(pid, title, y):
    return {"id": pid, "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def stat(pid, title, sql, x, y, w=4, h=4, unit=None, desc="", steps=None, decimals=2):
    return {
        "id": pid, "title": title, "type": "stat", "description": desc,
        "datasource": {"type": DS_TYPE, "uid": DS_UID},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [q(sql, 0)],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "area", "textMode": "auto",
                    "justifyMode": "auto", "orientation": "auto"},
        "fieldConfig": {"defaults": {
            "unit": unit or "none",
            "decimals": decimals,
            "thresholds": {"mode": "absolute",
                           "steps": steps or [{"color": "text", "value": None}]},
        }, "overrides": []},
    }


def ts(pid, title, sql, x, y, w, h, unit=None, desc="", bars=False, time_from=None):
    return {
        "id": pid, "title": title, "type": "timeseries", "description": desc,
        "datasource": {"type": DS_TYPE, "uid": DS_UID},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        **({"timeFrom": time_from} if time_from else {}),
        "targets": [q(sql, 1)],
        "options": {"legend": {"displayMode": "table", "placement": "bottom", "showLegend": True,
                               "calcs": ["mean", "lastNotNull"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "fieldConfig": {"defaults": {
            "custom": {"drawStyle": "bars" if bars else "line", "lineWidth": 1,
                       "fillOpacity": 25 if bars else 8, "showPoints": "never",
                       "spanNulls": True, "barAlignment": 0, "axisPlacement": "auto"},
            **( {"unit": unit} if unit else {} ),
            "color": {"mode": "palette-classic"},
        }, "overrides": []},
    }


def table(pid, title, sql, x, y, w=24, h=7, desc="", time_from=None):
    return {
        "id": pid, "title": title, "type": "table", "description": desc,
        "datasource": {"type": DS_TYPE, "uid": DS_UID},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        **(({"timeFrom": time_from} if time_from else {})),
        "targets": [q(sql, 0)],
        "options": {"showHeader": True, "cellHeight": "sm"},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "filterable": True}}, "overrides": []},
    }


def state_tl(pid, title, sql, x, y, w, h, desc="", time_from=None):
    """状态时间带：把一个数值码映射成有语义的色块（Grafana 11 原生 state-timeline）。"""
    return {
        "id": pid, "title": title, "type": "state-timeline", "description": desc,
        "datasource": {"type": DS_TYPE, "uid": DS_UID},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        **(({"timeFrom": time_from} if time_from else {})),
        "targets": [q(sql, 1)],
        "options": {"showValue": "auto", "mergeValues": True, "alignValue": "center",
                    "rowHeight": 0.85, "tooltip": {"mode": "single"},
                    "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}},
        "fieldConfig": {"defaults": {
            "custom": {"lineWidth": 0, "fillOpacity": 75},
            "mappings": [{"type": "value", "options": STATE_MAP}],
            "thresholds": {"mode": "absolute", "steps": [{"color": "text", "value": None}]},
        }, "overrides": []},
    }


STATE_MAP = {
    "0": {"text": "预热中", "color": "dark-gray", "index": 0},
    "1": {"text": "① 趋势·低波", "color": "green", "index": 1},
    "2": {"text": "② 趋势·高波", "color": "orange", "index": 2},
    "3": {"text": "③ 震荡·低波", "color": "blue", "index": 3},
    "4": {"text": "④ 震荡·高波", "color": "red", "index": 4},
}


# 第 0 屏：结论
P.append(stat(nid(), "IC 72h（7日均）", SQL_STAT_IC72, 0, 0, unit="none", decimals=4,
              desc="预测 value 与未来 72h 收益的日均 rank IC。>0.03 有截面区分力，<0 说明信号反了。",
              steps=[{"color": "red", "value": None}, {"color": "yellow", "value": 0},
                     {"color": "green", "value": 0.03}]))
P.append(stat(nid(), "ICIR（IC 均值/标准差）", SQL_STAT_ICIR, 4, 0, decimals=2,
              desc="IC 稳定性。>0.5 说明日度 IC 方向一致，不是靠几天暴击。",
              steps=[{"color": "red", "value": None}, {"color": "yellow", "value": 0.2},
                     {"color": "green", "value": 0.5}]))
P.append(stat(nid(), "多空价差 24h（日均 bp）", SQL_STAT_LS, 8, 0, unit="none", decimals=1,
              desc="多头组(9,10)−空头组(1,2) 的截面超额，已减去每小时全市场均值。>0 才叫吃到截面。",
              steps=[{"color": "red", "value": None}, {"color": "green", "value": 0}]))
P.append(stat(nid(), "截面离散度（最新小时 bp）", SQL_STAT_DISP, 12, 0, unit="none", decimals=1,
              desc="最新小时全市场 1h 收益的横截面标准差。这是「市场给了多大空间」。"))
P.append(stat(nid(), "策略 7 日收益（主账号 %）", SQL_STAT_RET7, 16, 0, unit="percent", decimals=2,
              desc="neutral_v1 主账号（43.153.152.139）近 7 日净值变化。",
              steps=[{"color": "red", "value": None}, {"color": "green", "value": 0}]))
P.append(stat(nid(), "收益↔离散度 相关性", SQL_STAT_CORR, 20, 0, decimals=3,
              desc="策略每小时收益与截面离散度的相关系数。显著为正=策略在截面分化大时赚得更多（真的在吃截面波动）；接近 0=赚的是别的；为负=反着来。",
              steps=[{"color": "red", "value": None}, {"color": "yellow", "value": 0},
                     {"color": "green", "value": 0.2}]))

y = 4
# ① 截面机会
P.append(row(nid(), "② 截面机会：市场给了多大空间", y)); y += 1
P.append(ts(nid(), "截面收益离散度（每小时）", SQL_DISPERSION, 0, y, 12, 8, unit="none",
            desc="std=全市场 1h 收益的横截面标准差；带宽=90 分位 − 10 分位。数值越大，截面越分化，选股空间越大。"))
P.append(ts(nid(), "日均截面离散度", SQL_DISPERSION_DAILY, 12, y, 12, 8, unit="none", bars=True,
            desc="把每小时离散度按天平均，看环境是不是单调变窄/变宽。"))
y += 8

# ② 信号效力
P.append(row(nid(), "③ 信号效力：预测能不能排出对的顺序", y)); y += 1
P.append(ts(nid(), "每日 Rank IC（1h / 24h / 72h）", SQL_IC_DAILY, 0, y, 12, 8,
            desc="模型 horizon 是 72h，看 IC 随 horizon 衰减的形状：1h 高=做短，72h 高=信号慢。"))
P.append(table(nid(), "10 分组 × 未来 24h 超额收益", SQL_DECILE, 12, y, 12, 8,
               desc="按预测值分 10 组，各组未来 24h 收益减去当期全市场均值。单调向上=排序有效；中间乱=只有两端有用。"))
y += 8
P.append(ts(nid(), "多空组每日超额（多 9,10 / 空 1,2）", SQL_LS_DAILY, 0, y, 12, 8, unit="none",
            desc="策略实际选的组：9,10 做多、1,2 做空（value 越大越看多）。蓝=多头组超额、橙=空头组超额、绿=多空价差。"))
_ic_panel = ts(nid(), "IC 72h：每日值 与 累计值", SQL_IC_CUM, 12, y, 12, 8, bars=True,
                desc="左轴柱=当日 rank IC（0.0x 量级）；右轴线=从窗口第一天累加（1.x 量级）。"
                     "两者量纲差一个数量级，不能比高低——看的是累计线是否单调向上、日柱是否长期为正。")
_ic_panel["fieldConfig"]["overrides"] = [{
    "matcher": {"id": "byName", "options": "累计IC(72h)"},
    "properties": [
        {"id": "custom.axisPlacement", "value": "right"},
        {"id": "custom.drawStyle", "value": "line"},
        {"id": "custom.fillOpacity", "value": 0},
        {"id": "custom.lineWidth", "value": 2},
    ],
}]
P.append(_ic_panel)
y += 8

# ③ 策略实盘
P.append(row(nid(), "④ 策略实盘：吃到了多少", y)); y += 1
P.append(ts(nid(), "净值曲线（各机器归一化，窗口起点=100）", SQL_WALLET, 0, y, 12, 8, unit="none",
            desc="三台机器本金差一个数量级（≈2.7k / 4.2k / 25k USDT），原值画在一张图上小账户会被压平。"
                 "这里各自归一化到窗口起点=100，看的是相对表现；绝对规模见右侧合计面板。"))
P.append(ts(nid(), "合计净值（USDT）", SQL_WALLET_TOTAL, 12, y, 12, 8, unit="usd",
            desc="三台机器净值按小时加总，看绝对规模的变化。"))
y += 8
P.append(table(nid(), "各机器表现（窗口内）", SQL_WALLET_STATS, 0, y, 12, 6,
               desc="起点/末值/区间收益/最大回撤。哪台机器明显跟不上，通常是持仓或成交链路的问题。"))
P.append(ts(nid(), "组合每小时收益（bp）", SQL_PORT_RET, 12, y, 12, 6, unit="none",
            desc="按各机器净值加权的小时收益。"))
y += 8
P.append(ts(nid(), "持仓宽度（3 cohort 合计，多/空 品种数）", SQL_POS_WIDTH, 0, y, 12, 8, unit="none",
            desc="策略每天 00:0x 写一次完整持仓快照，这里只取完整快照（行数 ≥100），所以是**日频**、点数=天数。宽度骤降=被强平/停开仓/信号空转。（小时级的零散行是止损/平仓事件，不代表全量持仓，已排除。）"))
P.append(ts(nid(), "策略收益 vs 截面离散度", SQL_RET_VS_DISP, 12, y, 12, 8, unit="none",
            desc="蓝=策略每小时收益，橙=当期截面离散度。离散度高的时段策略是否同步放大收益，是「吃到截面」的直接证据。"))
y += 8

# ④ 敞口与资金效率
# 策略是 3 天持有 × 3 个子策略轮转，每天只写一个 cohort 的快照。
# 只看单日快照会把 gross 低估 3 倍（我曾据此得出"仓位只有 31%"的错误结论）。
P.append(row(nid(), "⑤ 敞口与资金效率（3 cohort 合计）", y)); y += 1
P.append(stat(nid(), "仓位比例（gross/权益 %）", SQL_STAT_EXPO_RATIO, 0, y, 6, 4, unit="percent", decimals=1,
              desc="全部 cohort 的 gross 名义 / 账户权益。≈100% 即满仓；>100% 说明用了杠杆。",
              steps=[{"color": "red", "value": None}, {"color": "yellow", "value": 50},
                     {"color": "green", "value": 80}, {"color": "orange", "value": 120}]))
P.append(stat(nid(), "合计 gross（USDT）", SQL_STAT_GROSS, 6, y, 6, 4, unit="usd", decimals=0,
              desc="3 个 cohort 的持仓名义绝对值之和（多+空，不轧差）。"))
P.append(stat(nid(), "合计权益（USDT）", SQL_STAT_EQUITY, 12, y, 6, 4, unit="usd", decimals=0,
              desc="三台机器净值加总（ASOF 前向填充，避免某台未上报造成假跌）。"))
P.append(stat(nid(), "单 cohort gross（USDT）", SQL_STAT_COHORT, 18, y, 6, 4, unit="usd", decimals=0,
              desc="每个子策略的持仓名义。总 gross ÷ 这个数 = 当前有几个 cohort 在持。"))
y += 4
P.append(ts(nid(), "仓位比例随时间（gross/权益 %）", SQL_EXPO, 0, y, 12, 7, unit="percent",
            desc="100% = 满仓。曲线掉下来通常是调仓间隙（老 cohort 已平、新 cohort 未开）。"))
P.append(ts(nid(), "各 cohort gross（USDT）", SQL_COHORT_GROSS, 12, y, 12, 7, unit="usd",
            desc="三个子策略各自的名义规模。某条线归零 = 该 cohort 当日空仓/未调仓。"))
y += 7

# ⑤ 汇总
P.append(row(nid(), "⑥ 汇总", y)); y += 1
P.append(table(nid(), "近 7 日关键指标", SQL_SUMMARY, 0, y, 12, 6))
P.append(table(nid(), "说明", "SELECT '占位' AS a", 12, y, 12, 6))

# 用 text 面板写口径说明（不用 table 掺假数据）
P[-1] = {
    "id": P[-1]["id"], "type": "text", "title": "口径说明",
    "gridPos": {"x": 12, "y": y, "w": 12, "h": 6},
    "options": {"mode": "markdown", "content": (
        "**预测**：`quant.futures_prediction` / `lgmb_train2y_hr72_sharpe`，`value` 越大越看多。\n\n"
        "**前瞻收益**：`close(t+h)/close(t)-1`，按 `open_time` 对齐；未来 bar 缺失置 `nan` 而非 0，"
        "否则会把没数据算成暴跌。\n\n"
        "**超额**：每小时先减截面均值再分组，避免把大盘涨跌算成选股能力。\n\n"
        "**分组**：`value_group` 9/10=多、1/2=空（按 `future_position` 的 side×group 实测确认），10 分组是独立按 `value` 重排的视角。\n\n"
        "**实盘**：`future_wallet` 净值 → 按机器净值加权成组合小时收益；\n\n"
        "**⚠️ 出入金**：`value` 是账户权益，入金/出金会让它整千整万地跳（实测 2025-12-27 主账号 8,492→16,328、"
        "2026-07-13 12,720→22,993）。所有收益类面板都先把 |小时收益|>5% 的小时置 0（该策略小时 |收益| p99=1.81%，"
        "5% 只可能是转账）。不这样处理，日均会被从 5bp 拉到 300bp 以上、波动翻倍——"
        "旧版按原始净值算的「组合每小时收益」在这点上是有偏的。\n\n"
        "**风格价差**：每天 00:00(CST) 取一个截面，按因子分 5 组，Q5−Q1 的未来 24h 超额，再累加。"
        "00:00 让「前瞻 24h」正好等于策略按 `toDate` 记的自然日。\n\n"
        "**实盘**：`future_wallet` 净值 → 按机器净值加权成组合小时收益；"
        "`future_position` 按小时取 uniq pair 作为持仓宽度。"
    )},
}

# ② 市场状态与风格适配（插在 ① 之后：这是最该先看的一屏）
# 先把已有分区整体下移，把 y=4 让出来
SHIFT = 26
for _p in P:
    if _p["gridPos"]["y"] >= 4:
        _p["gridPos"]["y"] += SHIFT

y = 4
P.append(row(nid(), "① 市场状态与风格适配：现在是什么环境、策略在吃什么", y)); y += 1

_st = stat(nid(), "当前市场状态", SQL_STAT_STATE, 0, y, 6, 4, decimals=0,
           desc="由「趋势效率 ER」与「20日年化波动」两个 1 年分位阈值判定：\n"
                "① 趋势·低波 = ER>0.305 且波动≤54.2%（趋势策略最舒服）\n"
                "② 趋势·高波 = ER>0.305 但波动>54.2%（有方向但回撤大）\n"
                "③ 震荡·低波 = ER≤0.305 且波动≤54.2%（截面/反转更舒服）\n"
                "④ 震荡·高波 = ER≤0.305 且波动>54.2%（最难做，降低仓位）")
_st["fieldConfig"]["defaults"]["mappings"] = [{"type": "value", "options": STATE_MAP}]
P.append(_st)

P.append(stat(nid(), "站上 120h 均线的合约占比（%）", SQL_STAT_BREADTH, 6, y, 6, 4, unit="percent", decimals=1,
              desc="市场宽度。1 年分位 p10=30.3 / p50=41.6 / p90=56.3。\n"
                   "<35% 说明绝大多数合约在均线下方，做多截面要打折。",
              steps=[{"color": "red", "value": None}, {"color": "orange", "value": 35.5},
                     {"color": "yellow", "value": 41.6}, {"color": "green", "value": 47.4}]))
P.append(stat(nid(), "趋势效率 ER（20日）", SQL_STAT_ER, 12, y, 6, 4, decimals=3,
              desc="|20日累计收益| / 20日累计绝对收益。0=完全来回震荡，1=单边不回头。\n"
                   "1 年分位：p10=0.038 / p50=0.199 / p90=0.438。>0.305 视为趋势环境。",
              steps=[{"color": "blue", "value": None}, {"color": "yellow", "value": 0.12},
                     {"color": "green", "value": 0.305}]))
P.append(stat(nid(), "等权指数年化波动（%）", SQL_STAT_VOL, 18, y, 6, 4, unit="percent", decimals=1,
              desc="全 universe 等权组合日收益的 20 日标准差 × √365。\n"
                   "1 年分位：p10=32.1 / p50=49.6 / p90=86.3。>54.2 视为高波环境。",
              steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 42.3},
                     {"color": "orange", "value": 54.2}, {"color": "red", "value": 86.3}]))
y += 4

P.append(state_tl(nid(), "过去 120 天市场状态（每格 = 1 个交易日）", SQL_STATE_SERIES, 0, y, 24, 6, time_from="now-120d",
                  desc="四种状态的语义：① 趋势·低波（绿）/ ② 趋势·高波（橙）/ ③ 震荡·低波（蓝）/ ④ 震荡·高波（红）。\n"
                       "看的是「状态切换的频率」——频繁在 ①↔④ 之间跳说明环境不稳定，策略换手会被放大的区间。"))
y += 6

P.append(ts(nid(), "风格轮动：各风格多空价差的累计值（Q5−Q1，bp）", SQL_STYLE_CUM, 0, y, 12, 8, unit="none", time_from="now-120d",
            desc="每天 00:00 取一个截面，按该因子分 5 组，算 Q5−Q1 的未来 24h 超额，再累加。\n"
                 "线向上 = 这个风格当下有效（顺着它做赚钱）；向下 = 该风格被反向交易/失效。\n"
                 "动量/趋势强度上行 = 追涨有效；短期反转上行 = 抄跌有效；高波动上行 = 市场在给高波品种溢价。"))
P.append(table(nid(), "策略在吃什么风格（风格价差 ↔ 策略日收益）", SQL_STYLE_CORR, 12, y, 12, 8, time_from="now-180d",
               desc="正相关 = 策略与这个风格同向赚钱，即它实质上是一只「该风格」的策略；\n"
                    "接近 0 = 这块风格不吃；负相关 = 策略在逆着这个风格（做反转/做低波）。\n"
                    "Beta 是把风格价差当自变量回归策略收益的斜率：1bp 风格价差对应多少 bp 策略收益。"))
y += 8

P.append(table(nid(), "策略在不同市场状态下的表现（同日对齐）", SQL_STATE_PERF, 0, y, 12, 7, time_from="now-365d",
               desc="把每个交易日按当时状态分组，看策略日均收益/胜率/日波动。\n"
                    "「哪个状态赚钱、哪个状态亏钱、哪个状态波动最大」比总收益有用得多——\n"
                    "如果收益高度集中在 ①，那仓位管理就该按状态开关，而不是恒定仓位。"))
P.append(ts(nid(), "状态三指标：趋势效率 / 波动 / 宽度", SQL_STATE_TS, 12, y, 12, 7, unit="none",
            desc="三条线放在同一张图看「环境怎么变」：\n"
                 "ER 抬升 + 波动回落 = 舒服的趋势；ER 下行 + 波动抬升 = 混乱；\n"
                 "宽度（站上 MA120 占比）领先于价格，跌破 35% 通常意味着普跌还在延续。"))
y += 7

# 行容器按数组顺序吸收后续面板：必须按 (y, 行优先) 重排，否则新分区会被并进最后一行
P.sort(key=lambda _p: (_p["gridPos"]["y"], 0 if _p["type"] == "row" else 1))

DASH = {
    "uid": DASH_UID,
    "title": "QuantSupervise v2 · 截面盈利能力",
    "description": "围绕「截面机会 → 市场状态与风格 → 信号效力 → 实盘吃到多少」组织的策略监控。旧看板 QuantSupervise. 未改动。",
    "tags": ["quant", "strategy", "cross-section"],
    "timezone": "browser",
    "editable": True,
    "graphTooltip": 1,
    "refresh": "5m",
    "time": {"from": "now-14d", "to": "now"},
    "timepicker": {"refresh_intervals": ["1m", "5m", "15m", "1h"]},
    "templating": {"list": []},
    "annotations": {"list": []},
    "panels": P,
    "schemaVersion": 39,
    "version": 0,
}


def verify() -> int:
    bad = 0
    for p in P:
        if p["type"] in ("row", "text"):
            continue
        body = {"queries": [{
            "refId": "A", "datasource": {"type": DS_TYPE, "uid": DS_UID},
            "rawSql": p["targets"][0]["rawSql"], "format": p["targets"][0]["format"],
            "queryType": "sql", "intervalMs": 3600000, "maxDataPoints": 800,
        }], "from": p.get("timeFrom", "now-14d"), "to": "now"}
        try:
            res = api("/api/ds/query", "POST", body)
            node = res["results"]["A"]
            frames = node.get("frames", [])
            rows = 0
            for f in frames:
                vals = f.get("data", {}).get("values", [])
                if vals:
                    rows = max(rows, len(vals[0]))
            err = node.get("error")
            ok = rows > 0 and not err
            status = f"OK rows={rows}" if ok else f"FAIL {err or 'no rows'}"
            bad += 0 if ok else 1
        except Exception as e:                       # noqa: BLE001
            status, bad = f"EXC {str(e)[:110]}", bad + 1
        print(f"  [{p['id']:>2}] {p['title'][:46]:<48} {status}")
    return bad


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    body = {"dashboard": DASH, "overwrite": True,
            "message": "QuantSupervise v2：新增 ② 市场状态与风格适配（状态时间带 / 风格轮动 / 分状态表现 / 风格相关性）"}
    r = api("/api/dashboards/db", "POST", body)
    print(f"[deploy] {r.get('status')} uid={r.get('uid')} version={r.get('version')}")
    print(f"[deploy] {GF}/d/{DASH_UID}")
    if args.verify:
        print("\n[verify] 逐面板回读：")
        bad = verify()
        n = sum(1 for p in P if p["type"] not in ("row", "text"))
        print(f"\n[verify] 失败 {bad} / {n}")
        return 1 if bad else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
