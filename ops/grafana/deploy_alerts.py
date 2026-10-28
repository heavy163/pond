"""部署 cryptos 数据异常告警规则到 Grafana（幂等：按 title upsert）。

用法:
  /usr/bin/python3.12 ops/grafana/deploy_alerts.py            # 建全部
  /usr/bin/python3.12 ops/grafana/deploy_alerts.py --only 3   # 只建第 3 条（调试）

凭证: ~/.hermes/secrets/grafana_token
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path

GF = os.environ.get("GRAFANA_URL", "http://192.168.1.4:3000")
TOKEN = Path(os.environ.get("GRAFANA_TOKEN_FILE", "~/.hermes/secrets/grafana_token")).expanduser().read_text().strip()
DS_UID = "edvdze371o5q8f"
FOLDER_UID = "cg2kk23urwdmob"
GROUP = "cryptos-sync"


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


def rule(title: str, sql: str, severity: str, summary: str, for_: str,
         op: str = "gt", value: float = 0, no_data: str = "OK") -> dict:
    """一条规则 = 查询(A) → 取最后值(B) → 阈值(C)。"""
    return {
        "title": title,
        "ruleGroup": GROUP,
        "folderUID": FOLDER_UID,
        "orgID": 1,
        "condition": "C",
        "for": for_,
        "noDataState": no_data,
        "execErrState": "Alerting",
        "isPaused": False,
        "labels": {"severity": severity, "system": "cryptos"},
        "annotations": {"summary": summary},
        "data": [
            {
                "refId": "A",
                "relativeTimeRange": {"from": 600, "to": 0},
                "datasourceUid": DS_UID,
                "model": {
                    "refId": "A", "datasource": {"type": "grafana-clickhouse-datasource", "uid": DS_UID},
                    "rawSql": sql, "queryType": "sql", "rawQuery": True,
                    "format": 0, "intervalMs": 60000, "maxDataPoints": 1,
                },
            },
            {
                "refId": "B",
                "relativeTimeRange": {"from": 600, "to": 0},
                "datasourceUid": "__expr__",
                "model": {"refId": "B", "type": "reduce", "expression": "A", "reducer": "last"},
            },
            {
                "refId": "C",
                "relativeTimeRange": {"from": 600, "to": 0},
                "datasourceUid": "__expr__",
                "model": {
                    "refId": "C", "type": "threshold", "expression": "B",
                    "conditions": [{
                        "evaluator": {"params": [value], "type": op},
                        "operator": {"type": "and"},
                        "query": {"params": ["B"]},
                        "reducer": {"params": [], "type": "last"},
                        "type": "query",
                    }],
                },
            },
        ],
    }


RULES = [
    rule(
        "P1 整家交易所停更",
        """SELECT max(lag_h) FROM (
             SELECT venue, dateDiff('second', max(open_time), now()) / 3600 AS lag_h
             FROM cryptos.kline_futures_1h GROUP BY venue
           )""",
        "P1", "有交易所的 K线滞后超过 3 小时（停机或整家断更）", "5m", "gt", 3,
    ),
    rule(
        "P1 同步失败",
        """SELECT countIf(status != 'synced') FROM cryptos.sync_stat
           WHERE window_end = (SELECT max(window_end) FROM cryptos.sync_stat)""",
        "P1", "最近一轮有交易所状态不是 synced", "1m", "gt", 0,
    ),
    rule(
        "P1 出现未来/未收盘 bar",
        """SELECT
             (SELECT count() FROM cryptos.kline_futures_1h WHERE open_time > now() + INTERVAL 5 MINUTE)
           + (SELECT count() FROM cryptos.kline_spot_1h   WHERE open_time > now() + INTERVAL 5 MINUTE)""",
        "P1", "库里有未来时间戳的 bar（适配器口径错误，数据已污染）", "5m", "gt", 0,
    ),
    rule(
        "P1 永久丢失新增（当日）",
        """SELECT count() FROM cryptos.coverage FINAL
           WHERE status = 'lost' AND trade_date = today() AND data_type != 'oi'""",
        "P1", "K线/现货/funding 出现不可恢复缺失——这三类本该 100% 可补，lost 即架构异常", "10m", "gt", 0,
    ),
    rule(
        "P2 可补缺口新增",
        """SELECT count() FROM cryptos.coverage FINAL
           WHERE status = 'gap' AND trade_date >= today() - 1""",
        "P2", "有可修复缺口，等待每小时修复任务补回", "10m", "gt", 0,
    ),
    rule(
        "P2 跨所价差异常",
        """SELECT max(abs(bp)) FROM (
             SELECT venue, (close / md - 1) * 1e4 AS bp
             FROM (SELECT open_time, venue, close,
                          median(close) OVER (PARTITION BY open_time) AS md
                   FROM cryptos.kline_futures_1h
                   WHERE code = 'BTC-USDT' AND open_time >= now() - INTERVAL 2 HOUR)
           )""",
        "P2", "某交易所价格偏离同小时全所中位数过大（数据或合约有问题）", "5m", "gt", 50,
    ),
    rule(
        "P2 采集码数骤降",
        """SELECT max(100 * (1 - cur / nullIf(med, 0))) FROM (
             SELECT venue,
                    avgIf(codes, checked_at >= now() - INTERVAL 3 HOUR) AS cur,
                    quantile(0.5)(codes) AS med
             FROM cryptos.sync_stat
             WHERE checked_at >= now() - INTERVAL 7 DAY AND codes > 0
             GROUP BY venue
           )""",
        "P2", "某所采集码数比 7 日中位数下降超过 20%（换池未生效 / 标的池配置写错）", "10m", "gt", 20,
    ),
    rule(
        "P3 汇率异常",
        """SELECT abs(max(rate) / min(rate) - 1) * 100 FROM cryptos.fx_rate
           WHERE datetime >= now() - INTERVAL 1 DAY""",
        "P3", "KRW/USD 日变动超过 2%（会影响 Korea premium 口径）", "30m", "gt", 2,
    ),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", type=int, default=0, help="只部署第 N 条（1-based）")
    args = ap.parse_args()

    existing = {r["title"]: r for r in (api("/api/v1/provisioning/alert-rules") or [])
                if isinstance(r, dict) and "title" in r}
    todo = RULES[args.only - 1: args.only] if args.only else RULES

    failed = 0
    for r in todo:
        old = existing.get(r["title"])
        if old:
            res = api(f"/api/v1/provisioning/alert-rules/{old['uid']}", "PUT", r)
            act = "更新"
        else:
            res = api("/api/v1/provisioning/alert-rules", "POST", r)
            act = "新建"
        if "__error__" in res:
            failed += 1
            print(f"✗ {act} {r['title']}: HTTP {res['__error__']} {res['body'][:200]}")
        else:
            print(f"✓ {act} {r['title']}  uid={res.get('uid')}  severity={r['labels']['severity']}")

    back = api("/api/v1/provisioning/alert-rules")
    names = {x["title"] for x in back if isinstance(x, dict) and "title" in x}
    print(f"\nGrafana 现有规则 {len(names)} 条：")
    for n in sorted(names):
        print("  -", n)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
