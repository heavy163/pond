"""把 FutureData / FutureDataFv4 两张旧看板合并成一张新看板（不动旧看板）。

背景
----
旧的两张看板是同一批表的「行数随时间」图，区别只是查哪个库：
  FutureData     -> quant     （活库，每小时在写）
  FutureDataFv4  -> quant_v4  （静态副本，冻结在 2026-08-04）
合并后一张图里同时画两条线，副本落后/停更一眼就能看出来。

排版按数据性质分区：新鲜度 → K线 → 持仓情绪 → 因子 → 预测 → 汇总。
另外修掉旧看板的一个硬错：`FutureFactorV4Count` 查的是 `quant.factor_v4`，
这张表在 quant 里根本不存在（只在 quant_v4 有），那个面板一直是空的。
活库里对应的表叫 `quant.factor_online_v4`。

用法:
  /usr/bin/python3.12 ops/grafana/deploy_futuredata_dashboard.py
  /usr/bin/python3.12 ops/grafana/deploy_futuredata_dashboard.py --verify   # 逐面板回读
凭证: ~/.hermes/secrets/grafana_token
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
DASH_UID = "futuredata-overview"
FOLDER_UID = os.environ.get("GRAFANA_FOLDER_UID", "")     # 留空 = General


def api(path: str, method: str = "GET", body: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"{GF}{path}" if path.startswith("/api") else f"{GF}{path}",
        method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read().decode()
        return json.loads(raw) if raw else {}


def q(raw_sql: str, fmt: int = 1) -> dict:
    return {
        "refId": "A",
        "datasource": {"type": DS_TYPE, "uid": DS_UID},
        "rawSql": raw_sql,
        "queryType": "sql",
        "rawQuery": True,
        "format": fmt,
        "editorType": "sql",
    }


# ── SQL 模板 ─────────────────────────────────────────────────────
def two_db_sql(live_table: str, copy_table: str, tcol: str = "datetime") -> str:
    """同一张表在两个库里的每小时行数，输出 3 列 -> 两条线。"""
    return f"""
SELECT time,
       sumIf(c, src = 'live') AS quant_live,
       sumIf(c, src = 'copy') AS quant_v4_copy
FROM (
    SELECT $__timeInterval({tcol}) AS time, 'live' AS src, count() AS c
    FROM {live_table}
    WHERE {tcol} >= $__fromTime AND {tcol} <= $__toTime
    GROUP BY time
    UNION ALL
    SELECT $__timeInterval({tcol}) AS time, 'copy' AS src, count() AS c
    FROM {copy_table}
    WHERE {tcol} >= $__fromTime AND {tcol} <= $__toTime
    GROUP BY time
)
GROUP BY time ORDER BY time""".strip()


def one_db_hourly(table: str, tcol: str = "datetime") -> str:
    return f"""
SELECT $__timeInterval({tcol}) AS time, count() AS rows
FROM {table}
WHERE {tcol} >= $__fromTime AND {tcol} <= $__toTime
GROUP BY time ORDER BY time""".strip()


def daily_sql(table: str, tcol: str = "datetime", codes_col: str | None = None) -> str:
    metric = f"uniqExact({codes_col})" if codes_col else "count()"
    return f"""
SELECT toStartOfDay({tcol}) AS time, {metric} AS rows
FROM {table}
WHERE {tcol} >= $__fromTime AND {tcol} <= $__toTime
GROUP BY time ORDER BY time""".strip()


def lag_sql(table: str, tcol: str = "datetime", unit: str = "minute", div: int = 60) -> str:
    return f"""
SELECT round(dateDiff('{unit}', max({tcol}), now()) / {div}, 1) AS lag
FROM {table}""".strip()


# ── 面板构造 ─────────────────────────────────────────────────────
def row_panel(pid: int, title: str, y: int) -> dict:
    return {"id": pid, "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def ts_panel(pid, title, sql, x, y, w, h, unit=None, desc="", bars=False, stack=False, time_from=None):
    custom = {
        "drawStyle": "bars" if bars else "line",
        "lineWidth": 1, "fillOpacity": 30 if bars else 8,
        "showPoints": "never", "spanNulls": True,
        "stacking": {"mode": "normal", "group": "A"} if stack else {"mode": "none", "group": "A"},
        "barAlignment": 0, "axisPlacement": "auto", "axisLabel": "",
    }
    return {
        "id": pid, "title": title, "type": "timeseries", "description": desc,
        "datasource": {"type": DS_TYPE, "uid": DS_UID},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        **({"timeFrom": time_from} if time_from else {}),
        "targets": [q(sql, 1)],
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True,
                               "calcs": ["lastNotNull", "max"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "fieldConfig": {"defaults": {
            "custom": custom,
            **( {"unit": unit} if unit else {} ),
            "color": {"mode": "palette-classic"},
        }, "overrides": []},
    }


def stat_panel(pid, title, sql, x, y, w=3, h=4, unit="h", desc="",
               steps=((None, "green"), (3, "yellow"), (72, "red"))):
    return {
        "id": pid, "title": title, "type": "stat", "description": desc,
        "datasource": {"type": DS_TYPE, "uid": DS_UID},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [q(sql, 0)],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "background", "graphMode": "none",
                    "textMode": "value_and_name", "justifyMode": "auto", "orientation": "auto"},
        "fieldConfig": {"defaults": {
            "unit": unit,
            "thresholds": {"mode": "absolute",
                           "steps": [{"color": c, "value": v} for v, c in steps]},
        }, "overrides": []},
    }


def table_panel(pid, title, sql, x, y, w=24, h=8, desc=""):
    return {
        "id": pid, "title": title, "type": "table", "description": desc,
        "datasource": {"type": DS_TYPE, "uid": DS_UID},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [q(sql, 0)],
        "options": {"showHeader": True, "cellHeight": "sm",
                    "footer": {"show": False, "reducer": ["sum"], "countRows": False, "fields": ""}},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "filterable": True}}, "overrides": []},
    }


# ── 布局 ─────────────────────────────────────────────────────────
P: list[dict] = []
_pid = 0


def nid() -> int:
    global _pid
    _pid += 1
    return _pid


# 第 0 屏：新鲜度（8 个 stat，一眼看出哪条数据停了）
P.append(stat_panel(nid(), "合约K线 滞后", lag_sql("quant.kline_futures_1h"), 0, 0,
                    desc="quant.kline_futures_1h 最新 bar 距现在的小时数。>3h 黄、>72h 红。"))
P.append(stat_panel(nid(), "现货K线 滞后", lag_sql("quant.kline_spot_1h"), 3, 0,
                    desc="quant.kline_spot_1h"))
P.append(stat_panel(nid(), "持仓量OI 滞后", lag_sql("quant.future_open_interest"), 6, 0,
                    desc="quant.future_open_interest"))
P.append(stat_panel(nid(), "多空比 滞后", lag_sql("quant.future_long_short_position_ratio"), 9, 0,
                    desc="quant.future_long_short_position_ratio。该表 2025-12-01 后停更。"))
P.append(stat_panel(nid(), "因子V3 滞后", lag_sql("quant.`1h_generated_v3_V3_0_0`", "close_time"), 12, 0,
                    desc="quant.1h_generated_v3_V3_0_0"))
P.append(stat_panel(nid(), "因子V4 滞后", lag_sql("quant.factor_online_v4", "close_time"), 15, 0,
                    desc="quant.factor_online_v4（活库的 v4 因子表；quant 里没有 factor_v4）"))
P.append(stat_panel(nid(), "预测 滞后", lag_sql("quant.futures_prediction"), 18, 0,
                    desc="quant.futures_prediction"))
P.append(stat_panel(nid(), "quant_v4 副本 落后", lag_sql("quant_v4.kline_futures_1h", unit="day", div=1),
                    21, 0, unit="d", steps=((None, "green"), (7, "yellow"), (30, "red")),
                    desc="静态副本 quant_v4 冻结在 2026-08-04，天数会持续变大——这是预期行为，不是故障。"))

y = 4

# ① K线
P.append(row_panel(nid(), "① K线数据", y)); y += 1
P.append(ts_panel(nid(), "合约K线 每小时行数（quant 活库 vs quant_v4 副本）",
                  two_db_sql("quant.kline_futures_1h", "quant_v4.kline_futures_1h"), 0, y, 12, 8,
                  desc="两条线重合=副本还在跟；副本线在某天截断=冻结在该日。"))
P.append(ts_panel(nid(), "现货K线 每小时行数（quant vs quant_v4）",
                  two_db_sql("quant.kline_spot_1h", "quant_v4.kline_spot_1h"), 12, y, 12, 8))
y += 8
P.append(ts_panel(nid(), "合约K线 每日行数", daily_sql("quant.kline_futures_1h"), 0, y, 12, 7, bars=True,
                  desc="按 UTC+8 自然日汇总，柱子突然变矮=当天数据不全。"))
P.append(ts_panel(nid(), "现货K线 每日行数", daily_sql("quant.kline_spot_1h"), 12, y, 12, 7, bars=True))
y += 7

# ② 持仓与情绪
P.append(row_panel(nid(), "② 持仓与情绪（OI / 多空比）", y)); y += 1
P.append(ts_panel(nid(), "持仓量OI 每小时行数（quant vs quant_v4）",
                  two_db_sql("quant.future_open_interest", "quant_v4.future_open_interest"), 0, y, 12, 8))
P.append(ts_panel(nid(), "多空持仓比 每小时行数（quant vs quant_v4）",
                  two_db_sql("quant.future_long_short_position_ratio",
                             "quant_v4.future_long_short_position_ratio"), 12, y, 12, 8,
                  time_from="now-2y",
                  desc="quant 侧 2025-12-01 后停更、副本 2025-09-29 冻结，故本面板单独用 2 年窗口。"))
y += 8
P.append(ts_panel(nid(), "OI 每日覆盖码数", daily_sql("quant.future_open_interest", "datetime", "code"),
                  0, y, 12, 7, bars=True, desc="每天有多少个合约有 OI 快照。"))
P.append(ts_panel(nid(), "多空比 每日覆盖码数",
                  daily_sql("quant.future_long_short_position_ratio", "datetime", "code"),
                  12, y, 12, 7, bars=True, time_from="now-2y",
                  desc="同样用 2 年窗口：该表 2025-12-01 后无数据。"))
y += 7

# ③ 因子
P.append(row_panel(nid(), "③ 因子（V3 / V4）", y)); y += 1
P.append(ts_panel(nid(), "因子V3 每小时行数（quant vs quant_v4）",
                  two_db_sql("quant.`1h_generated_v3_V3_0_0`", "quant_v4.`1h_generated_v3_V3_0_0`",
                             "close_time"), 0, y, 12, 8))
P.append(ts_panel(nid(), "因子V4 每小时行数（quant.factor_online_v4 vs quant_v4.factor_v4）",
                  two_db_sql("quant.factor_online_v4", "quant_v4.factor_v4", "close_time"), 12, y, 12, 8,
                  time_from="now-1y",
                  desc="表名不一样：活库 factor_online_v4、副本 factor_v4。两张都停在 2026-08，故用 1 年窗口。"))
y += 8
P.append(ts_panel(nid(), "因子V3 每日覆盖码数",
                  daily_sql("quant.`1h_generated_v3_V3_0_0`", "close_time", "jj_code"), 0, y, 12, 7, bars=True))
P.append(ts_panel(nid(), "因子V4 每日覆盖码数",
                  daily_sql("quant.factor_online_v4", "close_time", "jj_code"), 12, y, 12, 7, bars=True,
                  time_from="now-1y", desc="同 V4 行数面板，用 1 年窗口。"))
y += 7

# ④ 预测
P.append(row_panel(nid(), "④ 预测（futures_prediction）", y)); y += 1
P.append(ts_panel(nid(), "预测 每小时行数（quant vs quant_v4）",
                  two_db_sql("quant.futures_prediction", "quant_v4.futures_prediction"), 0, y, 12, 8))
P.append(ts_panel(nid(), "预测 每日行数（按 model 拆分，quant）", f"""
SELECT toStartOfDay(datetime) AS time, model, count() AS rows
FROM quant.futures_prediction
WHERE datetime >= $__fromTime AND datetime <= $__toTime
GROUP BY time, model ORDER BY time""".strip(), 12, y, 12, 8,
                 desc="多模型并存时能看出哪个模型在写、哪天断的。"))
y += 8

# ⑤ 汇总
P.append(row_panel(nid(), "⑤ 汇总（近 30 日）", y)); y += 1
SUMMARY_TABLES = [
    ("合约K线", "kline_futures_1h", "datetime", "code", True),
    ("现货K线", "kline_spot_1h", "datetime", "code", True),
    ("持仓量OI", "future_open_interest", "datetime", "code", True),
    ("多空比", "future_long_short_position_ratio", "datetime", "code", True),
    ("因子V3", "1h_generated_v3_V3_0_0", "close_time", "jj_code", True),
    ("因子V4", "factor_online_v4", "close_time", "jj_code", False),
    ("预测", "futures_prediction", "datetime", "code", True),
]
parts = [
    f"""SELECT '{label}' AS kind, 'quant' AS db, count() AS rows,
        uniqExact({ccol}) AS codes, toString(max({tcol})) AS latest
 FROM quant.`{tab}` WHERE {tcol} >= now() - INTERVAL 30 DAY"""
    for label, tab, tcol, ccol, _ in SUMMARY_TABLES
]
parts += [
    f"""SELECT '{label}' AS kind, 'quant_v4' AS db, count() AS rows,
        uniqExact({ccol}) AS codes, toString(max({tcol})) AS latest
 FROM quant_v4.`{tab}` WHERE {tcol} >= now() - INTERVAL 30 DAY"""
    for label, tab, tcol, ccol, exists in SUMMARY_TABLES if exists
]
parts.append("""SELECT '因子V4' AS kind, 'quant_v4' AS db, count() AS rows,
        uniqExact(jj_code) AS codes, toString(max(close_time)) AS latest
 FROM quant_v4.factor_v4 WHERE close_time >= now() - INTERVAL 30 DAY""")
P.append(table_panel(nid(), "各数据类 近30日 行数 / 码数 / 最新时间", "\nUNION ALL\n".join(parts) + "\nORDER BY kind, db",
                     0, y, 24, 9,
                     desc="同一张表在活库和副本库里各有多少行、最新到什么时候。"))

DASH = {
    "uid": DASH_UID,
    "title": "FutureData 数据总览（quant 活库 + quant_v4 副本）",
    "description": "由 FutureData 与 FutureDataFv4 两张旧看板合并。按数据性质分区：新鲜度 → K线 → 持仓情绪 → 因子 → 预测 → 汇总。",
    "tags": ["futuredata", "quant", "data-quality"],
    "timezone": "browser",
    "editable": True,
    "graphTooltip": 1,
    "refresh": "1m",
    "time": {"from": "now-30d", "to": "now"},
    "timepicker": {"refresh_intervals": ["1m", "5m", "15m", "1h", "6h"]},
    "templating": {"list": []},
    "annotations": {"list": []},
    "panels": P,
    "schemaVersion": 39,
    "version": 0,
}


def verify() -> int:
    bad = 0
    for p in P:
        if p["type"] == "row":
            continue
        sql = p["targets"][0]["rawSql"]
        body = {
            "queries": [{
                "refId": "A", "datasource": {"type": DS_TYPE, "uid": DS_UID},
                "rawSql": sql, "format": p["targets"][0]["format"],
                "queryType": "sql", "intervalMs": 3600000, "maxDataPoints": 800,
            }],
            "from": p.get("timeFrom", "now-30d"), "to": "now",
        }
        try:
            res = api("/api/ds/query", "POST", body)
            frames = res["results"]["A"].get("frames", [])
            rows = 0
            for f in frames:
                vals = f.get("data", {}).get("values", [])
                if vals:
                    rows = max(rows, len(vals[0]))
            err = res["results"]["A"].get("error")
            status = f"OK rows={rows}" if rows and not err else f"FAIL {err or 'no rows'}"
            if not rows or err:
                bad += 1
        except Exception as e:                     # noqa: BLE001
            status = f"EXC {str(e)[:120]}"
            bad += 1
        print(f"  [{p['id']:>2}] {p['title'][:52]:<54} {status}")
    return bad


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()

    body = {"dashboard": DASH, "overwrite": True,
            "message": "合并 FutureData + FutureDataFv4：按数据性质分区，增加新鲜度与汇总统计"}
    if FOLDER_UID:
        body["folderUid"] = FOLDER_UID
    r = api("/api/dashboards/db", "POST", body)
    print(f"[deploy] {r.get('status')}  uid={r.get('uid')}  version={r.get('version')}")
    print(f"[deploy] {GF}/d/{DASH_UID}")

    if args.verify:
        print("\n[verify] 逐面板回读：")
        bad = verify()
        print(f"\n[verify] 失败 {bad} / {sum(1 for p in P if p['type'] != 'row')}")
        return 1 if bad else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
