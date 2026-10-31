"""把 cryptos 监控看板部署到 Grafana（幂等：按 uid 覆盖）。

用法: /usr/bin/python3.12 ops/grafana/deploy_dashboard.py
凭证: ~/.hermes/secrets/grafana_token（service account token）
"""

from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path

GF = os.environ.get("GRAFANA_URL", "http://192.168.1.4:3000")
TOKEN = Path(os.environ.get("GRAFANA_TOKEN_FILE", "~/.hermes/secrets/grafana_token")).expanduser().read_text().strip()
DS_UID = "edvdze371o5q8f"          # grafana-clickhouse-datasource
DASH_UID = "cryptos-monitor"


def api(path: str, method: str = "GET", body: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"{GF}{path}",
        method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def q(raw_sql: str, format_: int = 0) -> dict:
    """ClickHouse 查询节点。format_: 0=table, 1=timeseries。"""
    return {
        "refId": "A",
        "datasource": {"type": "grafana-clickhouse-datasource", "uid": DS_UID},
        "rawSql": raw_sql,
        "queryType": "sql",
        "rawQuery": True,
        "format": format_,
    }


def panel(pid: int, title: str, sql: str, x: int, y: int, w: int, h: int,
          ptype: str = "table", fmt: int = 0, unit: str | None = None,
          thresholds: list | None = None, desc: str = "") -> dict:
    targets = [q(sql, fmt)]
    targets[0]["refId"] = "A"
    p = {
        "id": pid, "title": title, "type": ptype, "description": desc,
        "datasource": {"type": "grafana-clickhouse-datasource", "uid": DS_UID},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "options": {},
    }
    if ptype == "table":
        p["options"] = {"showHeader": True, "sortBy": [{"displayName": title, "desc": False}]}
        p["fieldConfig"] = {"defaults": {"custom": {"align": "auto"}}, "overrides": []}
    elif ptype == "timeseries":
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "none"}}
        p["fieldConfig"] = {"defaults": {
            "custom": {"lineWidth": 1, "fillOpacity": 0, "showPoints": "never", "spanNulls": True},
            **({"unit": unit} if unit else {}),
        }, "overrides": []}
    elif ptype in ("stat", "gauge", "bargauge"):
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "orientation": "horizontal", "displayMode": "basic"}
        p["fieldConfig"] = {"defaults": {
            **({"unit": unit} if unit else {}),
            "thresholds": {"mode": "absolute", "steps": thresholds or [
                {"color": "green", "value": None},
                {"color": "red", "value": 1},
            ]},
        }, "overrides": []}
    return p


PANELS = [
    panel(
        1, "① 同步状态（最近一轮）",
        """SELECT * FROM (
               SELECT venue, status, rows, codes, duration_ms,
                      formatDateTime(checked_at, '%m-%d %H:%i:%S') AS checked,
                      formatDateTime(window_end, '%H:%i') AS round_h
               FROM cryptos.sync_stat
               ORDER BY checked_at DESC
               LIMIT 1 BY venue
           ) ORDER BY venue""",
        0, 0, 12, 8,
        desc=("每个交易所取它自己最近一次上报，恒为 9 行——轮次没跑完也不会消失。"
              "看 round_h 是否一致：某家 round_h 落后就说明它没跟上这一轮。"
              "status 不是 synced 就该看 journal。注意：synced 只代表请求没报错，不代表数据正确。"),
    ),
    panel(
        2, "② 数据新鲜度（滞后小时）",
        """SELECT venue, round(dateDiff('second', max(open_time), now()) / 3600, 2) AS lag_h
           FROM cryptos.kline_futures_1h GROUP BY venue
           UNION ALL
           SELECT venue, round(dateDiff('second', max(open_time), now()) / 3600, 2) AS lag_h
           FROM cryptos.kline_spot_1h GROUP BY venue
           ORDER BY lag_h DESC""",
        12, 0, 6, 8, ptype="bargauge", unit="h",
        thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 2.5},
                    {"color": "red", "value": 3.5}],
        desc="健康值约 1~2h（最新已收盘整点）。>2.5h 说明落后一轮，>3.5h 说明停更。",
    ),
    panel(
        3, "③ 未来/未收盘 bar（应为 0）",
        """SELECT
             (SELECT count() FROM cryptos.kline_futures_1h WHERE open_time > now() + INTERVAL 5 MINUTE) AS future_rows,
             (SELECT count() FROM cryptos.kline_futures_1h WHERE open_time >= toStartOfHour(now())) AS unclosed_rows""",
        18, 0, 6, 8, ptype="stat",
        thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}],
        desc="MEXC 曾把下一小时的 bar 写进库（247 行）。这一项 sync_stat 永远发现不了。",
    ),
    panel(
        4, "④ 覆盖率台账（ok / gap / lost / rampup）",
        """SELECT data_type, status, count() AS n
           FROM cryptos.coverage FINAL
           GROUP BY data_type, status
           ORDER BY data_type, status""",
        0, 8, 8, 7,
        desc="lost 是永久缺失（6 家快照所的 OI），不可恢复，下游必须排除样本。",
    ),
    panel(
        5, "⑤ 跨所价差（BTC，相对同小时中位数）",
        """SELECT open_time AS time, venue,
                  round((close / md - 1) * 1e4, 2) AS bp
           FROM (SELECT open_time, venue, close,
                        median(close) OVER (PARTITION BY open_time) AS md
                 FROM cryptos.kline_futures_1h
                 WHERE code = 'BTC-USDT' AND open_time >= now() - INTERVAL 24 HOUR)
           ORDER BY time""",
        8, 8, 10, 7, ptype="timeseries", fmt=1, unit="none",
        desc="正常在 ±2bp 内。某家持续偏离 >50bp = 该所数据或合约有问题。",
    ),
    panel(
        6, "⑥ 各所K线行数（近 24h）",
        """SELECT venue, count() AS rows
           FROM cryptos.kline_futures_1h
           WHERE open_time >= now() - INTERVAL 24 HOUR
           GROUP BY venue ORDER BY rows DESC""",
        18, 8, 6, 7,
        desc="码数 × 小时数。骤降说明换池或大量失败。",
    ),
    # ---------- 数据量可视化 ----------
    panel(
        7, "⑦ 各所每小时 K 线根数（近 24h）",
        """SELECT toStartOfHour(open_time) AS time, venue,
                  uniqExact(open_time, code) AS bars
           FROM cryptos.kline_futures_1h
           WHERE open_time >= now() - INTERVAL 24 HOUR
           GROUP BY time, venue ORDER BY time""",
        0, 15, 12, 8, ptype="timeseries", fmt=1,
        desc=("每根 K 线 = 一个 (code, open_time)，所以这应该是每个所一条近乎水平的线。"
              "必须用 uniqExact 而不是 count()：回看 6 小时会把同一批 bar 重写 6 遍，"
              "ReplacingMergeTree 在后台合并前 count() 会把同一根算 1.2~1.7 次，"
              "曲线随合并节奏上下跳——那是引擎的合并状态，不是数据量在变。"),
    ),
    panel(
        8, "⑧ 各所 × 数据类型 24h 行数",
        """SELECT venue,
                  sum(kline_fut) AS kline_fut,
                  sum(kline_spot) AS kline_spot,
                  sum(oi) AS oi,
                  sum(funding) AS funding
           FROM (
             SELECT venue, uniqExact(open_time, code) AS kline_fut, 0 AS kline_spot, 0 AS oi, 0 AS funding
             FROM cryptos.kline_futures_1h WHERE open_time >= now() - INTERVAL 24 HOUR GROUP BY venue
             UNION ALL SELECT venue, 0, uniqExact(open_time, code), 0, 0
             FROM cryptos.kline_spot_1h WHERE open_time >= now() - INTERVAL 24 HOUR GROUP BY venue
             UNION ALL SELECT venue, 0, 0, uniqExact(datetime, code), 0
             FROM cryptos.open_interest WHERE datetime >= now() - INTERVAL 24 HOUR GROUP BY venue
             UNION ALL SELECT venue, 0, 0, 0, uniqExact(datetime, code)
             FROM cryptos.funding_rate WHERE datetime >= now() - INTERVAL 24 HOUR GROUP BY venue
           ) GROUP BY venue ORDER BY venue""",
        12, 15, 12, 8,
        desc="现货所（upbit）没有 oi/funding，显示 0 属正常。",
    ),
    panel(
        9, "⑨ 各所采集码数（近 24h）",
        """SELECT toStartOfHour(open_time) AS time, venue, uniqExact(code) AS codes
           FROM cryptos.kline_futures_1h
           WHERE open_time >= now() - INTERVAL 24 HOUR
           GROUP BY time, venue ORDER BY time""",
        0, 23, 8, 7, ptype="timeseries", fmt=1,
        desc=("每小时的去重码数，基本是水平线。小幅波动来自该小时确实没有成交的合约"
              "（交易所不返回空 bar），实测 kucoin 695~703；断崖式跳变才是换池或停采。"),
    ),
    panel(
        10, "⑩ OI 每小时采集条数（近 24h）",
        """SELECT toStartOfHour(datetime) AS time, venue,
                  uniqExact(datetime, code) AS rows
           FROM cryptos.open_interest
           WHERE datetime >= now() - INTERVAL 24 HOUR
           GROUP BY time, venue ORDER BY time""",
        8, 23, 8, 7, ptype="timeseries", fmt=1,
        desc=("OI 是快照，每小时一轮、每个合约一条，所以应是水平线。同样用 uniqExact 而非 count()："
              "重写会让物理行翻倍（gate 实测 1.69x），合并前看着像暴涨。"
              "断档 = 那一小时永久丢失（6 家所无历史可补）。"
              "2026-11-01 前的 mexc 长期只有 ~860/1090，是「限流藏在 HTTP 200 里」的 bug，已修。"),
    ),
    panel(
        11, "⑪ 累计数据量（全库）",
        """SELECT (SELECT count() FROM cryptos.kline_futures_1h) AS kline_fut,
                  (SELECT count() FROM cryptos.kline_spot_1h)    AS kline_spot,
                  (SELECT count() FROM cryptos.open_interest)    AS oi,
                  (SELECT count() FROM cryptos.funding_rate)     AS funding""",
        16, 23, 8, 7, ptype="stat",
        desc="全量行数。仅作总量参考，异常要靠上面几张按所/按小时的图。",
    ),
]


# ---------------- 版面重排 ----------------
# 单所视图挪到第二屏（原来在 y=30，即第 3 屏，用户反馈找不到）
LAYOUT = {
    1: (0, 1),   2: (12, 1),  3: (18, 1),
    4: (0, 9),   5: (8, 9),   6: (18, 9),
    7: (0, 17),  8: (12, 17),
    9: (0, 25), 10: (8, 25), 11: (16, 25),
}
ROWS = [(0, "一、全所状态与健康"), (16, "二、全所数据量")]

for _p in PANELS:
    if _p["id"] in LAYOUT:
        _p["gridPos"]["x"], _p["gridPos"]["y"] = LAYOUT[_p["id"]]

ROW_PANELS = [{
    "id": 100 + i, "type": "row", "title": title, "collapsed": False,
    "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
    "panels": [],
} for i, (y, title) in enumerate(ROWS)]

# 行容器必须与它下面的面板交错排列，按 y 排序（同一 y 行在前）
PANELS = sorted(ROW_PANELS + PANELS,
                key=lambda _p: (_p["gridPos"]["y"], 0 if _p["type"] == "row" else 1, _p["gridPos"]["x"]))

for _p in PANELS:
    if _p["id"] in (12, 13):
        _p["timeFrom"] = "3d"

dashboard = {
    "uid": DASH_UID,
    "title": "Cryptos 多所数据同步监控",
    "tags": ["cryptos", "data-quality"],
    "timezone": "browser",
    "editable": True,
    "schemaVersion": 39,
    "refresh": "5m",
    "time": {"from": "now-24h", "to": "now"},
    "timepicker": {"refresh_intervals": ["1m", "5m", "15m", "1h"]},
    "panels": PANELS,
}

# 独立文件夹：与既有看板（FundETF/FutureData/...）分开，作为一个新的入口
FOLDER = "Cryptos"
folders = api("/api/folders?limit=100")
hit = next((f for f in folders if f["title"] == FOLDER), None)
if hit:
    folder_uid = hit["uid"]
else:
    folder_uid = api("/api/folders", "POST", {"title": FOLDER})["uid"]
print(f"[folder] {FOLDER} uid={folder_uid}")

res = api("/api/dashboards/db", "POST", {"dashboard": dashboard, "overwrite": True,
                                        "folderUid": folder_uid,
                                        "message": "deploy cryptos monitor"})
print("[deploy]", res.get("status"), res.get("uid"), "version", res.get("version"))
print(f"[url] {GF}/d/{res.get('uid')}")

# 回读确认
back = api(f"/api/dashboards/uid/{DASH_UID}")["dashboard"]
titles = [p["title"] for p in back["panels"]]
print("[verify] panels:", len(titles))
for t in titles:
    print("  -", t)
assert len(titles) == 13, titles  # 11 面板 + 2 个分区行
print("✅ 看板已部署并回读确认")
