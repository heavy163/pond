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
SELECT d, round(sumDaily, 4) AS "IC_72h_累计",
       round(runningAcc, 4) AS "累计" FROM (
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

SQL_WALLET = f"""
SELECT datetime AS time, machine AS "机器", value AS "净值"
FROM quant.future_wallet
WHERE strategy = '{STRATEGY}' AND datetime >= $__fromTime AND datetime <= $__toTime
ORDER BY time"""

SQL_PORT_RET = f"""
WITH w AS (
    SELECT machine, toStartOfHour(datetime) AS h, argMax(value, datetime) AS v
    FROM quant.future_wallet
    WHERE strategy = '{STRATEGY}' AND datetime >= $__fromTime AND datetime <= $__toTime
    GROUP BY machine, h
),
r AS (
    SELECT machine, h, v, v / lagInFrame(v) OVER (PARTITION BY machine ORDER BY h) - 1 AS ret
    FROM w
)
SELECT h AS time, round(sum(ret * v) / sum(v) * 1e4, 1) AS "组合每小时收益(bp)"
FROM r GROUP BY h HAVING isFinite(sum(ret * v)) ORDER BY time"""

SQL_POS_WIDTH = """
SELECT toStartOfHour(datetime) AS time,
       uniqExactIf(pair, side = 'long' AND status IN ('long', 'signal_exiting')) AS "多头持仓数",
       uniqExactIf(pair, side = 'short' AND status IN ('short', 'signal_exiting')) AS "空头持仓数"
FROM quant.future_position
WHERE datetime >= $__fromTime AND datetime <= $__toTime
GROUP BY time ORDER BY time"""

# 策略每小时收益 × 截面离散度（双轴）
SQL_RET_VS_DISP = CTE + f""",
w AS (
    SELECT machine, toStartOfHour(datetime) AS h, argMax(value, datetime) AS v
    FROM quant.future_wallet
    WHERE strategy = '{STRATEGY}' AND datetime >= $__fromTime AND datetime <= $__toTime
    GROUP BY machine, h
),
r AS (
    SELECT h, v, v / lagInFrame(v) OVER (PARTITION BY machine ORDER BY h) - 1 AS ret
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
r AS (SELECT h, v, v / lagInFrame(v) OVER (PARTITION BY machine ORDER BY h) - 1 AS ret FROM w),
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


def table(pid, title, sql, x, y, w=24, h=7, desc=""):
    return {
        "id": pid, "title": title, "type": "table", "description": desc,
        "datasource": {"type": DS_TYPE, "uid": DS_UID},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [q(sql, 0)],
        "options": {"showHeader": True, "cellHeight": "sm"},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "filterable": True}}, "overrides": []},
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
P.append(row(nid(), "① 截面机会：市场给了多大空间", y)); y += 1
P.append(ts(nid(), "截面收益离散度（每小时）", SQL_DISPERSION, 0, y, 12, 8, unit="none",
            desc="std=全市场 1h 收益的横截面标准差；带宽=90 分位 − 10 分位。数值越大，截面越分化，选股空间越大。"))
P.append(ts(nid(), "日均截面离散度", SQL_DISPERSION_DAILY, 12, y, 12, 8, unit="none", bars=True,
            desc="把每小时离散度按天平均，看环境是不是单调变窄/变宽。"))
y += 8

# ② 信号效力
P.append(row(nid(), "② 信号效力：预测能不能排出对的顺序", y)); y += 1
P.append(ts(nid(), "每日 Rank IC（1h / 24h / 72h）", SQL_IC_DAILY, 0, y, 12, 8,
            desc="模型 horizon 是 72h，看 IC 随 horizon 衰减的形状：1h 高=做短，72h 高=信号慢。"))
P.append(table(nid(), "10 分组 × 未来 24h 超额收益", SQL_DECILE, 12, y, 12, 8,
               desc="按预测值分 10 组，各组未来 24h 收益减去当期全市场均值。单调向上=排序有效；中间乱=只有两端有用。"))
y += 8
P.append(ts(nid(), "多空组每日超额（多 9,10 / 空 1,2）", SQL_LS_DAILY, 0, y, 12, 8, unit="none",
            desc="策略实际选的组：9,10 做多、1,2 做空（value 越大越看多）。蓝=多头组超额、橙=空头组超额、绿=多空价差。"))
P.append(ts(nid(), "IC 72h 累计", SQL_IC_CUM, 12, y, 12, 8,
            desc="日 IC 累加。持续向上=信号长期有效；走平或掉头=近期失效。"))
y += 8

# ③ 策略实盘
P.append(row(nid(), "③ 策略实盘：吃到了多少", y)); y += 1
P.append(ts(nid(), "净值曲线（neutral_v1 各机器）", SQL_WALLET, 0, y, 12, 8, unit="none",
            desc="多台机器的净值。曲线分叉=不同机器持仓/成交差异。"))
P.append(ts(nid(), "组合每小时收益（bp）", SQL_PORT_RET, 12, y, 12, 8, unit="none",
            desc="按各机器净值加权的小时收益。"))
y += 8
P.append(ts(nid(), "持仓宽度（多/空 品种数）", SQL_POS_WIDTH, 0, y, 12, 8, unit="none",
            desc="同时持有的品种数。宽度骤降=被强平/停开仓/信号空转。"))
P.append(ts(nid(), "策略收益 vs 截面离散度", SQL_RET_VS_DISP, 12, y, 12, 8, unit="none",
            desc="蓝=策略每小时收益，橙=当期截面离散度。离散度高的时段策略是否同步放大收益，是「吃到截面」的直接证据。"))
y += 8

# ④ 汇总
P.append(row(nid(), "④ 汇总", y)); y += 1
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
        "**实盘**：`future_wallet` 净值 → 按机器净值加权成组合小时收益；"
        "`future_position` 按小时取 uniq pair 作为持仓宽度。"
    )},
}

DASH = {
    "uid": DASH_UID,
    "title": "QuantSupervise v2 · 截面盈利能力",
    "description": "围绕「截面机会 → 信号效力 → 实盘吃到多少」组织的策略监控。旧看板 QuantSupervise. 未改动。",
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
            "message": "QuantSupervise v2：截面机会 → 信号效力 → 实盘吃到多少"}
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
