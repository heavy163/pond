"""单所数据量仪表盘（独立看板）：每类数据一块独立图表，顶部下拉切换交易所。

用法: /usr/bin/python3.12 ops/grafana/deploy_venue_dashboard.py
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

GF = os.environ.get("GRAFANA_URL", "http://192.168.1.4:3000")
TOKEN = Path(os.environ.get("GRAFANA_TOKEN_FILE", "~/.hermes/secrets/grafana_token")).expanduser().read_text().strip()
DS_UID = "edvdze371o5q8f"
DASH_UID = "cryptos-venue-volume"
FOLDER = "Cryptos"
V = "${venue:sqlstring}"


def api(path: str, method: str = "GET", body: dict | None = None):
    req = urllib.request.Request(
        f"{GF}{path}", method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode()
            return json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as e:
        return {"__error__": e.code, "body": e.read().decode()[:400]}


def panel(pid: int, title: str, sql: str, x: int, y: int, w: int, h: int,
          ptype: str = "timeseries", desc: str = "", fmt: int = 1) -> dict:
    p = {
        "id": pid, "title": title, "type": ptype, "description": desc,
        "datasource": {"type": "grafana-clickhouse-datasource", "uid": DS_UID},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{
            "refId": "A", "datasource": {"type": "grafana-clickhouse-datasource", "uid": DS_UID},
            "rawSql": sql, "queryType": "sql", "rawQuery": True, "format": fmt,
        }],
    }
    if ptype == "timeseries":
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "single", "sort": "none"}}
        p["fieldConfig"] = {"defaults": {
            "custom": {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never", "spanNulls": False},
            "unit": "short",
        }, "overrides": []}
    else:
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "orientation": "horizontal", "displayMode": "basic", "textMode": "auto"}
        p["fieldConfig"] = {"defaults": {"unit": "short"}, "overrides": []}
    return p


PANELS = [
    panel(
        1, "① 合约 K 线（kline_futures_1h）每小时根数",
        f"""SELECT toStartOfHour(open_time) AS time, uniqExact(code) AS bars
            FROM cryptos.kline_futures_1h
            WHERE venue = {V} AND open_time >= now() - INTERVAL 3 DAY
            GROUP BY time ORDER BY time""",
        0, 0, 12, 9,
        desc="每小时写入的合约 K 线根数，应≈该所合约码数。掉到 0/砍半=那小时采集失败；纯现货所无此表。",
    ),
    panel(
        2, "② 现货 K 线（kline_spot_1h）每小时根数",
        f"""SELECT toStartOfHour(open_time) AS time, uniqExact(code) AS bars
            FROM cryptos.kline_spot_1h
            WHERE venue = {V} AND open_time >= now() - INTERVAL 3 DAY
            GROUP BY time ORDER BY time""",
        12, 0, 12, 9,
        desc="只有现货所（upbit）有数据，合约所此处为空属正常。",
    ),
    panel(
        3, "③ 持仓量 OI（open_interest）每小时条数",
        f"""SELECT toStartOfHour(datetime) AS time, uniqExact(code) AS rows
            FROM cryptos.open_interest
            WHERE venue = {V} AND datetime >= now() - INTERVAL 3 DAY
            GROUP BY time ORDER BY time""",
        0, 9, 12, 9,
        desc="OI 是逐小时快照。断档=那一小时永久丢失（6 家快照所无历史接口，补不回来）。",
    ),
    panel(
        4, "④ 资金费率（funding_rate）每小时条数",
        f"""SELECT toStartOfHour(datetime) AS time, uniqExact(code) AS rows
            FROM cryptos.funding_rate
            WHERE venue = {V} AND datetime >= now() - INTERVAL 3 DAY
            GROUP BY time ORDER BY time""",
        12, 9, 12, 9,
        desc="资金费率通常 8h 结算一次，绝大多数小时为 0 属正常；结算时点应出现尖峰。",
    ),
    panel(
        5, "⑤ 四类数据 24h 汇总",
        f"""SELECT
              (SELECT uniqExact(open_time, code) FROM cryptos.kline_futures_1h
                WHERE venue = {V} AND open_time >= now() - INTERVAL 24 HOUR) AS kline_fut,
              (SELECT uniqExact(open_time, code) FROM cryptos.kline_spot_1h
                WHERE venue = {V} AND open_time >= now() - INTERVAL 24 HOUR) AS kline_spot,
              (SELECT uniqExact(datetime, code) FROM cryptos.open_interest
                WHERE venue = {V} AND datetime >= now() - INTERVAL 24 HOUR) AS oi,
              (SELECT uniqExact(datetime, code) FROM cryptos.funding_rate
                WHERE venue = {V} AND datetime >= now() - INTERVAL 24 HOUR) AS funding""",
        0, 18, 24, 5, ptype="stat", fmt=0,
        desc="近 24h 每类数据的逻辑行数（去重后）。",
    ),
]

dashboard = {
    "uid": DASH_UID,
    "title": "Cryptos 单所数据量",
    "tags": ["cryptos", "volume"],
    "timezone": "browser",
    "editable": True,
    "schemaVersion": 39,
    "refresh": "5m",
    "time": {"from": "now-3d", "to": "now"},
    "panels": PANELS,
    "templating": {
        "list": [{
            "name": "venue", "label": "交易所", "type": "query",
            "datasource": {"type": "grafana-clickhouse-datasource", "uid": DS_UID},
            "query": "SELECT DISTINCT venue FROM cryptos.sync_stat ORDER BY venue",
            "definition": "SELECT DISTINCT venue FROM cryptos.sync_stat ORDER BY venue",
            "refresh": 1, "sort": 1, "multi": False, "includeAll": False,
            "current": {"selected": True, "text": "okx", "value": "okx"},
        }]
    },
}

folders = api("/api/folders?limit=100")
hit = next((f for f in folders if f["title"] == FOLDER), None)
folder_uid = hit["uid"] if hit else api("/api/folders", "POST", {"title": FOLDER})["uid"]

res = api("/api/dashboards/db", "POST", {"dashboard": dashboard, "overwrite": True,
                                        "folderUid": folder_uid,
                                        "message": "deploy venue volume dashboard"})
print("[deploy]", res.get("status"), res.get("uid"), "version", res.get("version"))
print(f"[url] {GF}/d/{DASH_UID}")

back = api(f"/api/dashboards/uid/{DASH_UID}")["dashboard"]
titles = [p["title"] for p in back["panels"]]
print("[verify] panels:", len(titles))
for t in titles:
    print("  -", t)
assert len(titles) == 5, titles
print("✅ 单所数据量看板已部署")
