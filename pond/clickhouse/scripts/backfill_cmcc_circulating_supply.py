"""
CMCCirculatingSupply 回填脚本（backward fill）
================================================
算法
----
对每个标的，取其 CMCCirculatingSupply 的**首个正值**作为锚点（anchor），
然后把该标**锚点之前**的所有空值行，整段回填成这个常数。

    anchor_dt(code)  = min(datetime) where CMCCirculatingSupply > 0
    anchor_val(code) = CMCCirculatingSupply at anchor_dt

只填 `datetime < anchor_dt` 的行 —— 锚点之后的空洞不属于 backward 范围，
它们应该用后面的有效值来填（源数据里实际不存在这种空洞）。

为什么不用 close 比例外推
--------------------------
原方案 CMCC(t) = first_cmcc × close(t) / close(first_dt) 假设流通量∝价格。
实测 BTCUSDT：锚点 2025-10-01 CMCC=19,928,043，外推到 2020-09-01 得
2,027,738，而当日真实流通量约 18,500,000 —— 偏差约 9x。
流通量是缓慢单调变量，与价格无关，backward fill 常数更合理。

数据流
------
    source : quant.future_open_interest      （活表，首个正值作锚点）
    target : quant_v4.future_open_interest   （目标，全表 NaN）

关键实现细节（踩过的坑）
------------------------
1) 服务端 timezone() = Asia/Shanghai。SQL 字符串时间字面量按**服务端时区**
   解析，DateTime 列也返回 aware(Asia/Shanghai)。时间匹配一律用 epoch
   (toUnixTimestamp) 或显式 toDateTime64 字面量，不要拼裸字符串比较。

2) kline_futures_1h 的 datetime 是 K 线 **close_time**（xx:59:59.999），
   open_time 才是整点。两表对齐规则：
        join_key = toStartOfMinute(kline.datetime + INTERVAL 1 SECOND)
   即 "round(t + 1s, every=1min)"。用 open_time 去 join 看似 100% 匹配，
   实则错配到**下一根未来 K 线**（未来函数）。
   （本节算法不需要 klines，此条仅在需要价格时适用。）

3) 非 Nullable 的 Float64 列存不了 NULL，空值在源表里是 **NaN**。
   `IS NULL` 恒假，必须用 `isNaN()`。

4) `argMin(x, datetime)` 会被 NaN 传染 —— 428 个 code 因首行是 NaN 而整体
   返回 NaN。取值前必须先 `WHERE CMCCirculatingSupply > 0`（或 not isNaN）。

5) 源表 CMCC=0 是**占位符**（111 个 code 首值 0 之后才转正），所以锚点取
   严格正值 `> 0`，不是"首个非 NaN"。

6) ReplacingMergeTree 的 ORDER BY 是 (datetime, code)：回填用相同主键
   INSERT 覆盖旧行，但去重发生在 merge 时 —— 读之前 OPTIMIZE FINAL，
   或查询带 FINAL。

用法
----
    python backfill_cmcc_circulating_supply.py            # 只读：沙箱单测 + dry-run
    python backfill_cmcc_circulating_supply.py --execute  # 执行写入

    --src   源库（默认 quant）      --tgt  目标库（默认 quant_v4）
    --table 表名（默认 future_open_interest）

    例（同表自回填）：
    python backfill_cmcc_circulating_supply.py --src quant --tgt quant --execute
"""

import sys
from datetime import datetime, timedelta, timezone

import clickhouse_connect

UTC = timezone.utc

CHOST, CPORT, CUSER, CPWD = "192.168.1.200", 8123, "default", ""

DB_SRC = "quant"
DB_TGT = "quant_v4"
TBL = "future_open_interest"

COLS = ["datetime", "code", "sumOpenInterest", "sumOpenInterestValue",
        "CMCCirculatingSupply"]


def client():
    return clickhouse_connect.get_client(
        host=CHOST, port=CPORT, username=CUSER, password=CPWD, connect_timeout=20
    )


# ══════════════════════════════════════════════════════════════════
# 第 1 部分：沙箱单测
# ══════════════════════════════════════════════════════════════════

# 每个用例: (段列表[(小时数, cmcc)], 期望锚点, 期望回填行数, 期望剩余NaN, 说明)
#   cmcc=None 表示 NaN
#   回填规则：只填 datetime < anchor_dt 的 NaN 行
CASES = {
    "AAAUSDT": ([(24 * 30, None), (48, 1234.0)], 1234.0, 720, 48,
                "常规：720h空 + 锚点1234 → 填720，锚点后48h保持"),
    "BBBUSDT": ([(24 * 10, None), (10, 0.0), (48, 5678.0)], 5678.0, 250, 48,
                "0占位符：锚点须取5678 而非 0 → 填前250行"),
    "CCCUSDT": ([(24 * 10, None), (48, 0.0)], None, 0, 288,
                "无正值锚点：整体跳过，288行全保持 NaN"),
    "DDDUSDT": ([(48, 999.0)], 999.0, 0, 48,
                "锚点在第0行：之前无行可填，目标48行保持 NaN"),
    "EEEUSDT": ([(24 * 5, None), (24, 4321.0), (10, None)], 4321.0, 120, 34,
                "锚点后有空洞：只填锚点前120行，后34行保持 NaN"),
}


def build_rows(code, spec):
    rows, t = [], datetime(2025, 9, 1, tzinfo=UTC)
    # 每个 code 用不同的 OI 值，便于验证回填没有把它们覆盖掉
    soi = float(1000 + len(code))
    soiv = float(1e6 + len(code) * 7)
    for n, v in spec:
        for _ in range(n):
            rows.append([t, code, soi, soiv, float("nan") if v is None else float(v)])
            t += timedelta(hours=1)
    return rows


def sandbox_test(ch):
    print("=" * 72)
    print("第 1 部分：沙箱单测（quant_test，合成数据）")
    print("=" * 72)

    db, src, tgt = "quant_test", "quant_test.foi_src", "quant_test.foi_tgt"

    ch.query(f"DROP DATABASE IF EXISTS {db}")
    ch.query(f"CREATE DATABASE {db}")
    ddl = """
        datetime              DateTime64(3),
        code                  String,
        sumOpenInterest       Float64,
        sumOpenInterestValue  Float64,
        CMCCirculatingSupply  Float64
    ) ENGINE = ReplacingMergeTree
    PARTITION BY toYYYYMM(datetime)
    ORDER BY (datetime, code)
    """
    ch.query(f"CREATE TABLE {src} ({ddl}")
    ch.query(f"CREATE TABLE {tgt} ({ddl}")

    src_rows, tgt_rows = [], []
    for code, (spec, _av, _fc, _lc, _d) in CASES.items():
        for r in build_rows(code, spec):
            src_rows.append(r)
            tgt_rows.append([r[0], r[1], r[2], r[3], float("nan")])

    ch.insert(src, src_rows, column_names=COLS)
    ch.insert(tgt, tgt_rows, column_names=COLS)

    print(f"\n源 {len(src_rows)} 行 / 目标 {len(tgt_rows)} 行，{len(CASES)} 个边界用例:")
    for code, (_s, _av, _fc, _lc, desc) in CASES.items():
        print(f"  {code}: {desc}")

    # ── 求锚点（与生产路径共用实现）──
    anchors = fetch_anchors(ch, src)
    print(f"\n求得锚点 {len(anchors)} 个:")
    for c, (dt, v) in anchors.items():
        print(f"  {c}: {dt} -> {v:,.1f}")

    # ── 回填（与生产路径共用实现）──
    filled = backfill(ch, src, tgt, anchors, dry=False)
    print(f"\n回填 {sum(filled.values())} 行")

    # ── 验证 ──
    print("\n验证:")
    ok = True
    for code, (_spec, exp_av, exp_fill, exp_left, _desc) in CASES.items():
        a = anchors.get(code)
        got_av = a[1] if a else None
        got_fill = filled.get(code, 0)
        left = ch.query(f"""
            SELECT count() FROM {tgt} FINAL
            WHERE code='{code}' AND isNaN(CMCCirculatingSupply)
        """).result_rows[0][0]
        if a:
            pre_cond = "datetime < toDateTime64('%s', 3)" % a[0].strftime("%Y-%m-%d %H:%M:%S")
        else:
            pre_cond = "1"
        pre_nan = ch.query(f"""
            SELECT count() FROM {tgt} FINAL
            WHERE code='{code}' AND isNaN(CMCCirculatingSupply) AND ({pre_cond})
        """).result_rows[0][0]
        distinct_pre = ch.query(f"""
            SELECT uniqExact(CMCCirculatingSupply) FROM {tgt} FINAL
            WHERE code='{code}' AND ({pre_cond}) AND not isNaN(CMCCirculatingSupply)
        """).result_rows[0][0]

        if a is None:
            # 无锚点：不应填任何行，全部保持 NaN
            good = (got_av is None) and (got_fill == 0) and (left == exp_left) and (pre_nan == left)
        else:
            good = (got_av == exp_av) and (got_fill == exp_fill) and (left == exp_left)
            good = good and (pre_nan == 0) and (distinct_pre <= 1)

        # 关键回归检查：回填不得污染 sumOpenInterest / sumOpenInterestValue
        exp_soi = float(1000 + len(code))
        bad_oi = ch.query(f"""
            SELECT count() FROM {tgt} FINAL
            WHERE code='{code}' AND sumOpenInterest != {exp_soi}
        """).result_rows[0][0]
        good = good and (bad_oi == 0)
        ok &= good
        print(f"  {'✓' if good else '✗'} {code}: anchor={got_av} 填充={got_fill}(期望{exp_fill})"
              f" 剩余NaN={left}(期望{exp_left}) 锚点前残留NaN={pre_nan} 填充值种类={distinct_pre}"
              f" OI被污染行={bad_oi}")

    print(f"\n沙箱结论: {'全部通过 ✓' if ok else '存在失败 ✗'}")
    return ok


# ══════════════════════════════════════════════════════════════════
# 第 2 部分：真实表只读 dry-run
# ══════════════════════════════════════════════════════════════════

def dry_run(ch, src, tgt):
    print("\n" + "=" * 72)
    print(f"第 2 部分：只读 dry-run   {src}  ->  {tgt}")
    print("=" * 72)

    print("\n[1] 锚点（源表首个正值）")
    r = ch.query(f"""
        SELECT count() AS codes, min(v) AS mn, max(v) AS mx, avg(v) AS av
        FROM (
            SELECT code, argMin(CMCCirculatingSupply, datetime) AS v
            FROM {src} WHERE CMCCirculatingSupply > 0 GROUP BY code
        )
    """).result_rows[0]
    print(f"    有正值锚点的 code={r[0]}  min={r[1]:,.0f}  max={r[2]:,.0f}  avg={r[3]:,.0f}")

    print("\n[2] 待回填规模（锚点之前的 NaN 行，按年）")
    for x in ch.query(f"""
        SELECT toYear(t.datetime) y, count() c
        FROM {tgt} t
        JOIN (
            SELECT code, min(datetime) AS adt
            FROM {src} WHERE CMCCirculatingSupply > 0 GROUP BY code
        ) a ON t.code = a.code
        WHERE isNaN(t.CMCCirculatingSupply) AND t.datetime < a.adt
        GROUP BY y ORDER BY y
    """).result_rows:
        print(f"    {x[0]}: {x[1]:,}")

    print("\n[3] 无法回填的 code（目标有行、源无正值锚点）")
    un = ch.query(f"""
        SELECT v.code, count() AS tgt_rows
        FROM (SELECT DISTINCT code FROM {tgt}) v
        LEFT JOIN (SELECT code FROM {src} WHERE CMCCirculatingSupply > 0 GROUP BY code) s
               ON v.code = s.code
        WHERE s.code = ''
        GROUP BY v.code ORDER BY tgt_rows DESC
    """).result_rows
    print("    " + ("\n    ".join(f"{c}: {n:,} 行保持 NaN" for c, n in un) if un else "无"))

    print("\n[4] 试算样例（前 5 个 code）")
    for x in ch.query(f"""
        SELECT t.code, a.adt AS anchor_dt, a.av AS anchor_val, count() AS fill_rows,
               min(t.datetime) AS mn, max(t.datetime) AS mx
        FROM {tgt} t
        JOIN (
            SELECT code,
                   min(datetime) AS adt,
                   argMin(CMCCirculatingSupply, datetime) AS av
            FROM {src} WHERE CMCCirculatingSupply > 0 GROUP BY code
        ) a ON t.code = a.code
        WHERE isNaN(t.CMCCirculatingSupply) AND t.datetime < a.adt
        GROUP BY t.code, a.adt, a.av
        ORDER BY t.code LIMIT 5
    """).result_rows:
        print(f"    {x[0]:14s} anchor={x[1]} val={x[2]:>18,.0f} 填 {x[3]:>6,} 行  {x[4]} ~ {x[5]}")

    tot = ch.query(f"""
        SELECT count() FROM {tgt} t
        JOIN (SELECT code, min(datetime) AS adt FROM {src}
              WHERE CMCCirculatingSupply > 0 GROUP BY code) a ON t.code = a.code
        WHERE isNaN(t.CMCCirculatingSupply) AND t.datetime < a.adt
    """).result_rows[0][0]
    allnan = ch.query(f"SELECT count() FROM {tgt} WHERE isNaN(CMCCirculatingSupply)").result_rows[0][0]
    print(f"\n[5] 目标表 NaN 总计 {allnan:,} 行；其中可回填 {tot:,} 行，"
          f"不可回填 {allnan - tot:,} 行")
    return tot, allnan


def fetch_anchors(ch, src):
    """锚点：每个 code 的首个正值（0 是占位符，不能用'首个非NaN'）。"""
    return {
        c: (dt, v) for c, dt, v in ch.query(f"""
            SELECT code,
                   min(datetime)                          AS anchor_dt,
                   argMin(CMCCirculatingSupply, datetime)  AS anchor_val
            FROM {src}
            WHERE CMCCirculatingSupply > 0
            GROUP BY code
        """).result_rows
    }


def bt_condition(adt):
    """只回填锚点之前的行（backward 语义）。"""
    return f"datetime < toDateTime64('{adt:%Y-%m-%d %H:%M:%S}', 3)"


def backfill(ch, src, tgt, anchors, dry=True, progress_every=50):
    """按锚点向后回填。返回 {code: 回填行数}。

    注意：SQL SELECT * 方式读回原有的 sumOpenInterest / sumOpenInterestValue
    并原样写回 —— 绝不能写死常量，否则会覆盖这两列的原始数据。
    """
    filled = {}
    n = len(anchors)
    for i, (code, (adt, av)) in enumerate(anchors.items(), 1):
        rows = ch.query(f"""
            SELECT datetime, sumOpenInterest, sumOpenInterestValue
            FROM {tgt}
            WHERE code = '{code}' AND isNaN(CMCCirculatingSupply)
              AND {bt_condition(adt)}
            ORDER BY datetime
        """).result_rows
        if rows:
            filled[code] = len(rows)
            if not dry:
                ch.insert(tgt,
                          [[d, code, soi, soiv, av] for d, soi, soiv in rows],
                          column_names=COLS)
        if not dry and progress_every and i % progress_every == 0:
            print(f"    … {i}/{n} codes，累计 {sum(filled.values()):,} 行", flush=True)
    if not dry:
        ch.query(f"OPTIMIZE TABLE {tgt} FINAL")
    return filled


def production_backfill(ch, src, tgt):
    print("\n" + "=" * 72)
    print(f"第 3 部分：写入  {src}  ->  {tgt}")
    print("=" * 72)

    anchors = fetch_anchors(ch, src)
    print(f"锚点 {len(anchors)} 个，开始写入（ReplacingMergeTree 同主键覆盖）…")
    filled = backfill(ch, src, tgt, anchors, dry=False)
    print(f"已回填 {sum(filled.values()):,} 行，覆盖 {len(filled)} 个 code")

    left = ch.query(f"SELECT count() FROM {tgt} FINAL "
                    f"WHERE isNaN(CMCCirculatingSupply)").result_rows[0][0]
    print(f"回填后目标表剩余 NaN: {left:,} 行")
    return left


# ══════════════════════════════════════════════════════════════════

def parse_args(argv):
    opts = {"execute": False, "src": DB_SRC, "tgt": DB_TGT, "table": TBL}
    it = iter(argv)
    for a in it:
        if a == "--execute":
            opts["execute"] = True
        elif a in ("--src", "--tgt", "--table"):
            try:
                opts[a[2:]] = next(it)
            except StopIteration:
                raise SystemExit(f"{a} 需要一个值")
        else:
            raise SystemExit(f"未知参数: {a}")
    return opts


def main():
    o = parse_args(sys.argv[1:])
    src = f'{o["src"]}.{o["table"]}'
    tgt = f'{o["tgt"]}.{o["table"]}'

    ch = client()
    ok = sandbox_test(ch)
    dry_run(ch, src, tgt)

    if o["execute"]:
        if not ok:
            print("\n沙箱未通过，拒绝写入。")
            ch.close()
            sys.exit(1)
        production_backfill(ch, src, tgt)
    else:
        print("\n（只读模式；加 --execute 才写入）")

    ch.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
