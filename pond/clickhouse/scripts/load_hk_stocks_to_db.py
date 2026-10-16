"""港股日线回填（→ quant.hk_kline_1d / quant.hk_index_1d / quant.hk_stock_info）。

为什么需要这张表
----------------
ClickHouse `quant` 库原有 47 张表里【一张港股表都没有】—— 全部是 A股 / 商品期货 /
加密 / 股东 / 代币。研究港股（个股、恒指择时、港股通成分股）之前必须先建数据层。

数据源
------
- 个股日线：akshare.stock_hk_hist（东方财富），**后复权**。
  选东财不选新浪的理由：新浪 stock_hk_daily 不返回成交额/换手率，东财给
  后复权 + 成交量 + 成交额 + 换手率，字段更全。
- 指数日线：akshare.stock_hk_index_daily_em。源列名是 latest（不是 close），映射到 close。
- 快照信息：akshare.stock_hk_spot_em（当前在市 4737 只，含名称/成交额/市值/PE）。

⚠️ 已知偏差（报告里必须写明）
----------------------------
1. **幸存者偏差**：universe 取自【当前在市】列表。已退市港股拿不到 ⇒
   「港股全市场」回测系统性高估。
2. **零成交标的**：4737 只里 2608 只（55%）当日成交额为 0，成交额 >1000 万港币的
   只有 461 只 ⇒ 研究侧必须先做流动性 universe。
3. **快照非时点**：hk_stock_info 只有当前快照，不能回答历史某日有哪些股票。

长跑架构（同 load_futures_contracts_to_db.py，血泪教训勿改）
-------------------------------------------------------------
每块 = 一批代码（默认 120 只）= 【独立子进程】+ 外部硬超时 + 进程组 SIGKILL
+ 重试 3 次 + 单块失败不中断整体。原因：东财接口在长驻进程里会退化（socket 损坏
→ 全线程 futex 死锁 → 既不报错也不退出），必须靠外部杀进程兜底。

单块内【失败率】守门：某块失败率 > 20% 即抛错让驱动重试。失败率高说明接口退化，
此时"部分成功"比"整块失败"更危险 —— 会静默丢数（同 load_futures_contracts 的教训）。

用法
----
    # 看有哪些块
    PYTHONPATH=<pond> /usr/bin/python3.12 load_hk_stocks_to_db.py --list

    # 跑单块（父进程就是这么调自己）
    ... --batch 3

    # 指数 + 快照（一次性，很便宜）
    ... --indices --info

    # 全量（分块 + 超时 + 重试），用 systemd-run 隔离内存
    systemd-run --user --scope -q -p MemoryMax=4G --collect \
        env PYTHONPATH=<pond> CLICKHOUSE_HOST=192.168.1.200 /usr/bin/python3.12 \
        load_hk_stocks_to_db.py --all --timeout 900 --retries 3
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import date, datetime

import pandas as pd

# ── 常量 ─────────────────────────────────────────────────────────────────────
BATCH_SIZE = 120
START_DATE = "20000101"          # 要最长历史；源按上市日自然截断，不会造假数据
INDICES = ["HSI", "HSTECH", "HSCEI"]

KLINE_COLS = ["datetime", "code", "open", "high", "low", "close",
              "volume", "amount", "turn"]
INDEX_COLS = ["datetime", "code", "open", "high", "low", "close"]
INFO_COLS = ["datetime", "code", "name", "board", "last", "amount"]

# 单块失败率上限：超过即抛错重试（防止接口退化时的静默丢数）
MAX_FAIL_RATE = 0.20


def _log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ── universe ─────────────────────────────────────────────────────────────────
def fetch_universe() -> pd.DataFrame:
    """当前在市港股快照。返回 code(5 位补零)/name/board/last/amount/mktcap/pe。"""
    import akshare as ak
    s = ak.stock_hk_spot_em()
    if s is None or len(s) == 0:
        raise RuntimeError("stock_hk_spot_em 返回空 —— 不做静默跳过")
    d = s.copy()
    d["code"] = d["代码"].astype(str).str.zfill(5)
    d["name"] = d["名称"].astype(str)
    # ⚠️ GEM 是 08xxx（前两位 08），不是前三位 008：00883 中海油是【主板】。
    #    曾写成 [:3]=="008" 导致 GEM 只统计到 82 只（实际 338 只）。
    d["board"] = d["code"].str[:2].map(lambda p: "GEM" if p == "08" else "MAIN")
    # ⚠️ 源只有 12 列，【没有】总市值/市盈率 —— 不造空列（同 BondKlineDaily）
    for src, dst in (("最新价", "last"), ("成交额", "amount")):
        d[dst] = pd.to_numeric(d.get(src), errors="coerce") if src in d.columns else float("nan")
    return d


def universe_codes() -> list[str]:
    """universe 代码表。优先读 quant.hk_stock_info 的快照（秒级），
    表为空时才回退到实时拉取 —— 因为 stock_hk_spot_em 一次要 ~52 秒，
    而分块驱动每个子进程都要用它，40 块就是 35 分钟纯开销。

    把 DB 作为 universe 的单一真相源还有第二个好处：快照被永久记录，
    「这次回填到底覆盖了哪些代码」可事后复核。
    """
    try:
        mgr = _manager()
        df = mgr.native_sql_read_table(
            "SELECT DISTINCT code FROM quant.hk_stock_info "
            "WHERE datetime = (SELECT max(datetime) FROM quant.hk_stock_info)")
        if df is not None and len(df):
            codes = sorted(df["code"].astype(str).tolist())
            if codes:
                return codes
    except Exception as e:                              # noqa: BLE001
        _log(f"读取 hk_stock_info 失败（回退实时拉取）: {type(e).__name__} {str(e)[:80]}")
    uni = fetch_universe()
    return sorted(uni["code"].tolist())


def code_batches(n: int | None = None) -> list[list[str]]:
    """把 universe 切成块。块内容对同一 universe 是确定的（按代码排序后切片）。"""
    codes = universe_codes()
    if n is not None:
        codes = codes[:n]
    return [codes[i:i + BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]


# ── 数据获取 ─────────────────────────────────────────────────────────────────
def fetch_code(code: str, ed: str) -> pd.DataFrame:
    """单只港股全历史（后复权）。失败抛异常，由调用方计数。"""
    import akshare as ak
    h = ak.stock_hk_hist(symbol=code, period="daily",
                         start_date=START_DATE, end_date=ed, adjust="hfq")
    if h is None or len(h) == 0:
        return pd.DataFrame()
    d = h.copy()
    d["code"] = code
    return d


def to_orm_kline(df: pd.DataFrame) -> pd.DataFrame:
    """东财中文列 → ORM 列。⚠️ 必须只保留合法行，否则 close<=0 会污染回测。"""
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=KLINE_COLS)
    d = df.copy()
    d["datetime"] = pd.to_datetime(d["日期"], errors="coerce")
    ren = {"开盘": "open", "最高": "high", "最低": "low", "收盘": "close",
           "成交量": "volume", "成交额": "amount", "换手率": "turn"}
    for src, dst in ren.items():
        d[dst] = pd.to_numeric(d[src], errors="coerce") if src in d.columns else float("nan")
    d = d.dropna(subset=["datetime", "close"])
    # ⚠️ 必须过滤【全部】非正 OHLC，不只是 close/open：
    #    实测 low<=0 有 2583 行（占 0.03%），全部出现在【合股/拆细日】——
    #    同一行里 open 用了复权前的价、close 用了复权后的价。实例：
    #      00521 2026-10-15: open 0.005 / high 0.156 / low -0.034 / close 0.072
    #    负数的 low 会污染任何用 high-low 或 low 算的波动率/日内幅度因子。
    #    这些值【不修正】（修正 = 编数据），直接丢行；丢多少行记录在报告里。
    d = d[(d[["open", "high", "low", "close"]] > 0).all(axis=1)]
    d = d[d["high"] >= d["low"]]                    # high<low 物理不可能（实测 5 行）
    d["code"] = d["code"].astype(str)
    return d[KLINE_COLS].reset_index(drop=True)


def to_orm_index(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """指数：源列 date/open/high/low/latest → datetime/.../close。"""
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=INDEX_COLS)
    d = df.copy()
    d["datetime"] = pd.to_datetime(d["date"], errors="coerce")
    d["close"] = pd.to_numeric(d["latest"], errors="coerce")
    for c in ("open", "high", "low"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d["code"] = code
    d = d.dropna(subset=["datetime", "close"])
    d = d[d["close"] > 0]
    return d[INDEX_COLS].reset_index(drop=True)


# ── 写库 ─────────────────────────────────────────────────────────────────────
def _manager():
    from pond.clickhouse.manager import ClickHouseManager
    host = os.environ.get("CLICKHOUSE_HOST", "192.168.1.200")
    db = os.environ.get("CLICKHOUSE_DB", "quant")
    pwd = os.environ.get("CLICKHOUSE_PWD", "")
    return ClickHouseManager(
        f"clickhouse://default:{pwd}@{host}:8123/{db}",
        data_start=datetime(2015, 1, 1),
        native_uri=f"clickhouse+native://default:{pwd}@{host}:9000/{db}?tcp_keepalive=true",
    )


def _save(table_cls, df: pd.DataFrame, trunk: int = 200_000) -> int:
    if df is None or len(df) == 0:
        return 0
    return _manager().save_to_db(table_cls, df, last_record_filters=None,
                                 trunk_size=trunk) or 0


# ── 块执行 ───────────────────────────────────────────────────────────────────
def run_batch(bi: int, n: int | None = None) -> int:
    """跑第 bi 块（0-based），返回写入行数。"""
    from pond.clickhouse.kline import HkKline1d
    batches = code_batches(n)
    if not 0 <= bi < len(batches):
        raise SystemExit(f"块号 {bi} 超范围（共 {len(batches)} 块）")
    codes = batches[bi]
    ed = date.today().strftime("%Y%m%d")
    t0 = time.time()
    frames, failed = [], []
    for cd in codes:
        try:
            f = fetch_code(cd, ed)
            if len(f):
                frames.append(f)
        except Exception as e:                          # noqa: BLE001
            failed.append(f"{cd}:{type(e).__name__}")
    fr = len(failed) / max(len(codes), 1)
    df = to_orm_kline(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
    _log(f"batch {bi}: 源 {sum(len(f) for f in frames)} 行 → 清洗后 {len(df)} 行 "
         f"({df['code'].nunique() if len(df) else 0} 只) 失败 {len(failed)}/{len(codes)} "
         f"({fr*100:.0f}%) {time.time()-t0:.1f}s")
    if failed[:5]:
        _log(f"  ⚠️ 失败样例: {failed[:5]}")
    if fr > MAX_FAIL_RATE:
        # 「部分成功」= 静默丢数，比整块失败更危险 ⇒ 抛错让驱动重试
        raise RuntimeError(f"batch {bi} 失败率 {fr*100:.0f}% > {MAX_FAIL_RATE*100:.0f}% "
                           f"⇒ 判定接口退化，抛错重试（不做部分写入）")
    if len(df) == 0:
        _log(f"batch {bi}: 空块，跳过（不视为失败）")
        return 0
    rows = _save(HkKline1d, df)
    _log(f"batch {bi}: 写入 {rows} 行 {time.time()-t0:.1f}s")
    return rows


def run_indices() -> int:
    """基准指数一次性回填。"""
    import akshare as ak
    from pond.clickhouse.kline import HkIndex1d
    total = 0
    for k in INDICES:
        try:
            d = ak.stock_hk_index_daily_em(symbol=k)
        except Exception as e:                          # noqa: BLE001
            _log(f"index {k}: ❌ {type(e).__name__} {str(e)[:80]}")
            continue
        df = to_orm_index(d, k)
        rows = _save(HkIndex1d, df)
        total += rows
        _log(f"index {k}: {len(d)} 行 → 写入 {rows} 行 "
             f"{df['datetime'].min().date() if len(df) else '-'} → "
             f"{df['datetime'].max().date() if len(df) else '-'}")
    return total


FX_PAIRS = {"港币": "HKDCNY", "美元": "USDCNY"}


def run_fx() -> int:
    """人民币汇率中间价（港股以港币计价，RMB 视角回测必需）。"""
    import akshare as ak
    from pond.clickhouse.kline import FxRate1d
    total = 0
    for sym, code in FX_PAIRS.items():
        try:
            d = ak.currency_boc_sina(symbol=sym, start_date=START_DATE,
                                     end_date=date.today().strftime("%Y%m%d"))
        except Exception as e:                          # noqa: BLE001
            _log(f"fx {code}: ❌ {type(e).__name__} {str(e)[:80]}")
            continue
        if d is None or len(d) == 0:
            _log(f"fx {code}: 空，跳过")
            continue
        df = pd.DataFrame({
            "datetime": pd.to_datetime(d["日期"], errors="coerce"),
            "code": code,
            "rate": pd.to_numeric(d["央行中间价"], errors="coerce"),
        }).dropna(subset=["datetime", "rate"])
        df = df[df["rate"] > 0]
        rows = _save(FxRate1d, df)
        total += rows
        _log(f"fx {code}: {len(d)} 行 → 写入 {rows} 行 "
             f"{df['datetime'].min().date()} → {df['datetime'].max().date()}")
    return total


def run_info() -> int:
    """当前在市快照（同时是 universe 的永久记录）。"""
    from pond.clickhouse.kline import HkStockInfo
    uni = fetch_universe()
    snap = pd.Timestamp(date.today())
    df = uni.copy()
    df["datetime"] = snap
    df = df[INFO_COLS]
    rows = _save(HkStockInfo, df)
    _log(f"info: {len(uni)} 只 → 写入 {rows} 行（快照日 {snap.date()}）")
    return rows


# ── 市值 / 估值 ──────────────────────────────────────────────────────────────
# 百度股市通的 period 会【降采样】，返回点数上限约 1,100：
#   近一年=365(日) 近三年=1096(日) 近五年=913(≈2日) 近十年=731(周) 全部=628(2004起,双周)
# 时点研究取「全部」覆盖最长；月度调仓下前值填充足够。
VALUATION_INDICATORS = {"总市值": "mcap", "市盈率(TTM)": "pe_ttm", "市净率": "pb"}


def fetch_valuation(code: str) -> dict[str, pd.Series]:
    """单只港股的 市值/PE/PB 全历史（失败抛异常，由调用方计数）。"""
    import akshare as ak
    out: dict[str, pd.Series] = {}
    for ind, col in VALUATION_INDICATORS.items():
        d = ak.stock_hk_valuation_baidu(symbol=code, indicator=ind, period="全部")
        if d is None or len(d) == 0:
            continue
        idx = pd.to_datetime(d["date"], errors="coerce")
        vals = pd.to_numeric(d["value"], errors="coerce")
        s = pd.Series(vals.to_numpy(), index=idx).dropna()
        if len(s):
            out[col] = s[~s.index.duplicated(keep="last")]
    return out


def run_valuation_batch(bi: int, n: int | None = None) -> int:
    from pond.clickhouse.kline import HkValuation1d
    codes = code_batches(n)[bi]
    frames, ok, bad = [], 0, []
    for c in codes:
        try:
            v = fetch_valuation(c)
        except Exception:                                   # noqa: BLE001
            bad.append(c)
            continue
        if v.get("mcap") is None or len(v) == 0:
            bad.append(c)
            continue
        df = pd.DataFrame(v)
        df.index.name = "datetime"
        df = df.reset_index()
        df["code"] = c
        frames.append(df)
        ok += 1
    if not frames:
        _log(f"val batch {bi}: 无数据（{len(codes)} 只全失败）")
        return 0
    allf = pd.concat(frames, ignore_index=True)
    for col in ("mcap", "pe_ttm", "pb"):          # 必须含全部 ORM 列
        if col not in allf:
            allf[col] = float("nan")
    allf = allf.dropna(subset=["datetime"])
    allf = allf[allf["mcap"].fillna(0) > 0]
    rows = _save(HkValuation1d, allf)
    _log(f"val batch {bi}: {ok}/{len(codes)} 只有数据 → 写入 {rows} 行"
         + (f"  失败 {len(bad)}: {bad[:8]}" if bad else ""))
    return rows


# ── 分块驱动 ─────────────────────────────────────────────────────────────────
def _drive(flag: str, timeout: int, retries: int, n: int | None = None,
           label: str = "batch") -> int:
    """通用分块驱动：每块独立子进程 + 外部硬超时 + 进程组 SIGKILL + 重试 + 不中断整体。"""
    batches = code_batches(n)
    _log(f"共 {len(batches)} 块（每块 {BATCH_SIZE} 只），单块超时 {timeout}s，"
         f"失败重试 {retries} 次")
    ok, failed = [], []
    for i in range(len(batches)):
        done = False
        for attempt in range(1, retries + 1):
            t0 = time.time()
            proc = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__), flag, str(i)]
                + (["--n", str(n)] if n else []),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                start_new_session=True,
            )
            try:
                out, _ = proc.communicate(timeout=timeout)
                if proc.returncode == 0:
                    _log(f"[{i+1}/{len(batches)}] ✅ {label} {i}  "
                         f"尝试 {attempt}/{retries}  {time.time()-t0:.1f}s")
                    ok.append(i); done = True
                    break
                _log(f"[{i+1}/{len(batches)}] ⚠️ {label} {i} rc={proc.returncode} "
                     f"尝试 {attempt}/{retries}  尾部: {out.strip()[-300:]}")
            except subprocess.TimeoutExpired:
                _log(f"[{i+1}/{len(batches)}] ⏱ {label} {i} TIMEOUT {timeout}s → 杀进程组")
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:                       # noqa: BLE001
                    proc.kill()
                try:
                    proc.communicate(timeout=30)
                except Exception:                       # noqa: BLE001
                    pass
            if attempt < retries:
                time.sleep(5)
        if not done:
            _log(f"[{i+1}/{len(batches)}] ❌ {label} {i} 重试 {retries} 次仍失败 → "
                 f"【不中断整体】，继续下一块")
            failed.append(i)
    _log(f"== 完成：成功 {len(ok)}/{len(batches)}，失败 {len(failed)} ==")
    if failed:
        _log(f"失败块: {failed}（用 {flag} <i> 单独重跑）")
    return 0 if not failed else 1


def run_all(timeout: int, retries: int, n: int | None = None) -> int:
    return _drive("--batch", timeout, retries, n, "batch")


def run_valuation_all(timeout: int, retries: int, n: int | None = None) -> int:
    return _drive("--val-batch", timeout, retries, n, "val")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, help="跑单块（0-based）")
    ap.add_argument("--n", type=int, default=None, help="只用前 N 只（测试用）")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--indices", action="store_true")
    ap.add_argument("--info", action="store_true")
    ap.add_argument("--fx", action="store_true")
    ap.add_argument("--valuation", action="store_true", help="全市场市值/PE/PB 回填（分块）")
    ap.add_argument("--val-batch", type=int, help="跑单块市值（0-based，内部用）")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--retries", type=int, default=3)
    a = ap.parse_args()

    if a.list:
        for i, b in enumerate(code_batches(a.n)):
            print(f"batch {i}: {len(b)} 只  {b[0]} … {b[-1]}")
        return 0
    if a.all:
        return run_all(a.timeout, a.retries, a.n)
    if a.valuation:
        return run_valuation_all(a.timeout, a.retries, a.n)
    if a.batch is not None:
        run_batch(a.batch, a.n)
        return 0
    if a.val_batch is not None:
        run_valuation_batch(a.val_batch, a.n)
        return 0
    if a.indices:
        run_indices()
    if a.info:
        run_info()
    if a.fx:
        run_fx()
    if not (a.indices or a.info or a.fx):
        ap.error("需要 --all / --list / --batch / --indices / --info / --fx / --valuation")
    return 0


if __name__ == "__main__":
    sys.exit(main())
