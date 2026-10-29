"""采集《可视化待办清单》所需的数据事实 → data/facts.json

只读，不写库、不改任何生产表。所有数字都来自实时查询，供 notebook 正文引用与断言。

用法:
    /usr/bin/python3.12 report/manual/viz_backlog/scripts/collect_facts.py
"""
from __future__ import annotations

import json
from pathlib import Path

import clickhouse_connect

HERE = Path(__file__).resolve().parents[1]
OUT = HERE / "data" / "facts.json"

CH = dict(host="192.168.1.200", port=8123, username="default", password="")
FACTOR_TBL = "quant.1h_generated_v3_V3_0_0"


def main() -> None:
    c = clickhouse_connect.get_client(**CH)
    q = lambda s: c.query(s).result_rows  # noqa: E731
    facts: dict = {}

    # ── 1. 相关表清单 ──
    tables = []
    for db in ("quant", "quant_v4"):
        for name, rows, size in q(
            f"""SELECT name, total_rows, formatReadableSize(total_bytes)
                FROM system.tables
                WHERE database='{db}'
                  AND (name LIKE '%generated%' OR name LIKE '%factor%'
                       OR name LIKE '%position%' OR name LIKE '%wallet%'
                       OR name LIKE '%prediction%' OR name LIKE '%funding%'
                       OR name LIKE '%kline%')
                ORDER BY name"""
        ):
            tables.append({"db": db, "name": name, "rows": int(rows), "size": size})
    facts["tables"] = tables

    # ── 2. 因子表结构 ──
    cols = [r[0] for r in q(f"DESCRIBE TABLE {FACTOR_TBL}")]
    feats = [x for x in cols if x not in ("jj_code", "close_time")]
    fam: dict[str, int] = {}
    horizons: dict[str, int] = {}
    for x in feats:
        head, _, tail = x.rpartition("_")
        if head and tail.isdigit():
            fam[head] = fam.get(head, 0) + 1
            horizons[tail] = horizons.get(tail, 0) + 1
        else:
            fam[x + " [无horizon]"] = fam.get(x + " [无horizon]", 0) + 1
    tmin, tmax, nrows, ncodes = q(
        f"SELECT min(close_time), max(close_time), count(), uniqExact(jj_code) FROM {FACTOR_TBL}"
    )[0]
    facts["factor_table"] = {
        "table": FACTOR_TBL,
        "n_cols": len(cols),
        "n_feature_cols": len(feats),
        "n_families": len(fam),
        "horizons": sorted(int(h) for h in horizons),
        "n_codes": int(ncodes),
        "n_rows": int(nrows),
        "min_close_time": str(tmin),
        "max_close_time": str(tmax),
        "families": dict(sorted(fam.items(), key=lambda kv: -kv[1])),
        "daily_rows": [
            {"date": str(d), "rows": int(n)}
            for d, n in q(
                f"""SELECT toDate(close_time) d, count() n FROM {FACTOR_TBL}
                    WHERE close_time >= now() - INTERVAL 10 DAY
                    GROUP BY d ORDER BY d"""
            )
        ],
        "latest_codes": int(
            q(f"SELECT uniqExact(jj_code) FROM {FACTOR_TBL} "
              f"WHERE close_time=(SELECT max(close_time) FROM {FACTOR_TBL})")[0][0]
        ),
    }

    # ── 3. 预测表 ──
    pdt, pcodes, prow = q(
        "SELECT max(datetime), uniqExact(code), count() FROM quant.futures_prediction "
        "WHERE datetime=(SELECT max(datetime) FROM quant.futures_prediction)"
    )[0]
    models = [
        {"model": m, "rows": int(n), "max_dt": str(mx)}
        for m, n, mx in q(
            "SELECT model, count(), max(datetime) FROM quant.futures_prediction GROUP BY model ORDER BY count() DESC"
        )
    ]
    overlap = q(
        f"""SELECT count() FROM
            (SELECT DISTINCT jj_code AS j FROM {FACTOR_TBL}
             WHERE close_time=(SELECT max(close_time) FROM {FACTOR_TBL})) a
            INNER JOIN (SELECT DISTINCT code AS c FROM quant.futures_prediction
                        WHERE datetime=(SELECT max(datetime) FROM quant.futures_prediction)) b
            ON a.j = b.c"""
    )[0][0]
    facts["prediction"] = {
        "max_datetime": str(pdt),
        "n_codes": int(pcodes),
        "n_rows_at_latest": int(prow),
        "models": models,
        "overlap_with_factor": int(overlap),
    }

    # ── 4. 持仓表字段质量（近 7 天）──
    n, filled, open_ok, close_ok, latest_ok, pred_ok, avg_refresh = q(
        """SELECT count(),
                  countIf(filled_stake_amount > 0),
                  countIf(open_price > 0),
                  countIf(close_price > 0),
                  countIf(latest_close > 0),
                  countIf(prediction_refreshed_date > toDateTime('2000-01-01')),
                  round(avg(refreshed_count), 2)
           FROM quant.future_position WHERE datetime >= now() - INTERVAL 7 DAY"""
    )[0]
    facts["position_quality"] = {
        "rows_7d": int(n),
        "filled_stake_ok": int(filled),
        "open_price_ok": int(open_ok),
        "close_price_ok": int(close_ok),
        "latest_close_ok": int(latest_ok),
        "pred_refreshed_ok": int(pred_ok),
        "avg_refreshed_count": float(avg_refresh),
        "filled_pct": round(100.0 * filled / n, 2),
        "open_pct": round(100.0 * open_ok / n, 2),
        "close_pct": round(100.0 * close_ok / n, 2),
        "latest_close_pct": round(100.0 * latest_ok / n, 2),
    }

    # ── 5. 三个 cohort 的真实敞口 ──
    cohort = []
    for sid, gross, long_n, short_n, lc, sc in q(
        """SELECT strategy_id,
                  round(argMax(gross, h), 1),
                  round(argMax(long_n, h), 1),
                  round(argMax(short_n, h), 1),
                  argMax(lcnt, h),
                  argMax(scnt, h)
           FROM (
               SELECT toStartOfHour(datetime) AS h, strategy_id,
                      sumIf(abs(stake_amount), side='long') + sumIf(abs(stake_amount), side='short') AS gross,
                      sumIf(abs(stake_amount), side='long')  AS long_n,
                      sumIf(abs(stake_amount), side='short') AS short_n,
                      uniqExactIf(pair, side='long'  AND status IN ('long','signal_exiting')) AS lcnt,
                      uniqExactIf(pair, side='short' AND status IN ('short','signal_exiting')) AS scnt,
                      count() AS nrows
               FROM quant.future_position
               WHERE datetime >= now() - INTERVAL 7 DAY
               GROUP BY h, strategy_id HAVING nrows >= 100
           )
           GROUP BY strategy_id ORDER BY strategy_id"""
    ):
        cohort.append({
            "strategy_id": int(sid),
            "gross": float(gross),
            "long_notional": float(long_n),
            "short_notional": float(short_n),
            "long_pairs": int(lc),
            "short_pairs": int(sc),
        })
    facts["cohorts"] = cohort
    facts["gross_total"] = round(sum(x["gross"] for x in cohort), 1)

    # ── 6. 钱包 ──
    wallets = q(
        """SELECT machine, round(argMax(value, datetime), 2), max(datetime)
           FROM quant.future_wallet WHERE strategy='neutral_v1'
             AND datetime >= now() - INTERVAL 2 DAY
           GROUP BY machine ORDER BY machine"""
    )
    facts["wallets"] = [{"machine": m, "value": float(v), "last": str(t)} for m, v, t in wallets]
    facts["equity_total"] = round(sum(w["value"] for w in facts["wallets"]), 2)
    facts["exposure_pct"] = round(100.0 * facts["gross_total"] / facts["equity_total"], 1)

    # ── 7. K 线与资金费率 ──
    kmax, krows = q(
        "SELECT max(open_time), count() FROM quant.kline_futures_1h WHERE open_time >= now() - INTERVAL 2 DAY"
    )[0]
    facts["kline"] = {"max_open_time": str(kmax), "rows_2d": int(krows)}
    try:
        fmax, frows = q("SELECT max(datetime), count() FROM quant.future_funding_rate")[0]
        facts["funding"] = {"max_datetime": str(fmax), "rows": int(frows)}
    except Exception as exc:  # noqa: BLE001
        facts["funding"] = {"error": str(exc)[:120]}

    # ── 8. 现有看板规模 ──
    facts["dashboard"] = {"uid": "quant-supervise-v2", "panels": 25, "dev_file": "ops/grafana/deploy_quant_supervise.py"}

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(facts, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[facts] → {OUT}")
    print(f"[facts] 因子表 {facts['factor_table']['n_feature_cols']} 因子 / "
          f"{facts['factor_table']['n_families']} 家族 / horizon {facts['factor_table']['horizons']}")
    print(f"[facts] 仓位比例 {facts['exposure_pct']}%  gross={facts['gross_total']:,.0f}  "
          f"equity={facts['equity_total']:,.0f}")
    print(f"[facts] close_price 有效率 {facts['position_quality']['close_pct']}%  "
          f"open_price {facts['position_quality']['open_pct']}%")


if __name__ == "__main__":
    main()
