"""东方财富数据中心 —— 股东持仓代理。

⚠️ 与同目录其他 proxy 不同：本数据是【按报告期的截面】，不是【按标的的时序】，
所以**不继承 `DataProxy`**（它的 get_klines(symbol, period, adjust, start, end)
接口形状套不上，硬套会让调用方以为可以按 symbol 拉）。真实接口是：

    get_periods()             → 待拉取的报告期列表
    fetch(end_date, report)   → 该报告期的全量截面（列名已映射到 ORM 列名）

分块驱动 `run_chunked_backfill.py` 的 SYMBOL_OFFSET/SYMBOL_LIMIT 在这里被解释为
**报告期维度**的切片，而不是标的切片。

═══════════════════════════════════════════════════════════════════════════════
实测约束（2026-10，全部踩过）
═══════════════════════════════════════════════════════════════════════════════
1. **`pageNumber > 100` 恒返回空**。`pageSize=500` 时单期 108 页，第 101~108 页
   重试 4 次仍空 ⇒ 整期少 3,846 行（7.7%）。改用 `pageSize=2000` ⇒ 27 页全可用。
   本模块在 pages > MAX_PAGE 时**直接报错**，不静默截断。
2. **单页会随机返回 `result=None`** ⇒ 必须页级重试。不重试会让「代码集合差」类
   分析得出假阳性（实测把 000001 平安银行算成「已退市」）。
3. **字符串 range filter 不被支持**（`(SECURITY_CODE>='600000')` 直接返回空）。
   报告期若要拆查询，只能按 `HOLDER_TYPE` 之类的类别字段拆。
4. **字段填充率差异极大，映射选错就丢 2/3 数据**（2024-09-30 全量实测）：
      HOLD_NUM_CHANGE 100%  vs  XZCHANGE         33.4%
      NEW_CHANGE_RATIO 100% vs  CHANGE_RATIO     33.2%
   ⇒ 数量变化/变化比例必须取前者。
5. **`NOTICE_DATE` 与 `UPDATE_DATE` 100% 相等**，两者都表示（可能是修订后的）
   公告日。用它做时点口径是**保守**的（只会晚不会早），符合无未来函数要求。
6. `HOLDER_CODE` 只有 66.6% 填充 ⇒ **不能**当股东实体主键，要用 `HOLDER_NAME`。
"""
import time

import pandas as pd
import requests

URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
PAGE_SIZE = 2000
MAX_PAGE = 100                 # 实测硬上限
RETRY = 4
SLEEP = 0.4

# 报表名 → 表名 / 列映射（源列 → ORM 列）
REPORTS: dict[str, dict] = {
    "free": {
        "report_name": "RPT_F10_EH_FREEHOLDERS",
        "table": "FreeHoldingDetail",
        "colmap": {
            "SECURITY_CODE": "code",
            "HOLDER_NAME": "name",
            "HOLDER_TYPE": "nature",
            "HOLD_NUM": "volume",
            "HOLDER_MARKET_CAP": "free_amount",
            "HOLD_NUM_CHANGE": "volume_change",          # ← 不是 XZCHANGE（33% 填充）
            "NEW_CHANGE_RATIO": "volume_change_percent",  # ← 不是 CHANGE_RATIO
            "END_DATE": "report_date",
            "UPDATE_DATE": "publish_date",
            "HOLDER_RANK": "holder_rank",
            "HOLDER_NEWTYPE": "holder_newtype",
            "SHARES_TYPE": "shares_type",
            "FREE_HOLDNUM_RATIO": "free_holdnum_ratio",
            "HOLDER_CODE": "holder_code",
            "SECURITY_NAME_ABBR": "security_name",
        },
    },
    "top": {
        "report_name": "RPT_F10_EH_HOLDERS",
        "table": "HoldingDetail",
        "colmap": {
            "SECURITY_CODE": "code",
            "HOLDER_NAME": "name",
            "HOLDER_NEW": "nature",
            "HOLD_NUM": "volume",
            "HOLDER_MARKET_CAP": "amount",
            "HOLD_NUM_CHANGE": "volume_change",
            "NEW_CHANGE_RATIO": "volume_change_percent",
            "END_DATE": "report_date",
            "HOLDER_RANK": "holder_rank",
            "SHARES_TYPE": "shares_type",
            "HOLDER_CODE": "holder_code",
            "SECURITY_NAME_ABBR": "security_name",
            # 十大股东表无 UPDATE_DATE / NOTICE_DATE，公告日需调用方补
        },
    },
}


def _request(params: dict) -> dict:
    last = None
    for _ in range(RETRY):
        try:
            j = requests.get(URL, params=params, timeout=30).json()
            if j.get("result"):
                return j
            last = j.get("message") or "result=None"
        except Exception as e:                                            # noqa: BLE001
            last = f"{type(e).__name__}: {str(e)[:120]}"
        time.sleep(SLEEP)
    raise RuntimeError(f"东财取数失败: {last}")


class EastMoneyHolderProxy:
    def __init__(self, report: str = "free") -> None:
        if report not in REPORTS:
            raise ValueError(f"report 必须是 {list(REPORTS)} 之一，得到 {report!r}")
        self.report = report
        self.cfg = REPORTS[report]

    # ── 报告期 ───────────────────────────────────────────────────────────────
    def get_periods(self, start: str = "2005-01-01", end: str | None = None) -> list[str]:
        """生成季度报告期列表（YYYY-MM-DD）。空期由 fetch 返回空 df 自然跳过。"""
        end = end or pd.Timestamp.today().strftime("%Y-%m-%d")
        per = pd.date_range(start, end, freq="QE").strftime("%Y-%m-%d").tolist()
        return per

    # ── 单报告期全量 ─────────────────────────────────────────────────────────
    def fetch(self, end_date: str) -> pd.DataFrame:
        """拉一个报告期的全量截面，列名已映射到 ORM 列名。

        完整性问题一律【抛异常】而不是返回残缺 df —— 残缺数据比没有数据更危险。
        """
        base = {
            "reportName": self.cfg["report_name"], "columns": "ALL",
            "source": "WEB", "client": "WEB",
            "sortColumns": "UPDATE_DATE,SECURITY_CODE,HOLDER_RANK",
            "sortTypes": "-1,1,1",
            "filter": f"(END_DATE='{end_date}')",
        }
        head = _request(dict(base, pageSize=str(PAGE_SIZE), pageNumber="1"))
        res = head["result"]
        expect, pages = int(res["count"]), int(res["pages"])
        if expect == 0:
            return pd.DataFrame()
        if pages > MAX_PAGE:
            raise RuntimeError(
                f"{end_date} 需要 {pages} 页 > 上限 {MAX_PAGE}。"
                f"必须按 HOLDER_TYPE 拆查询（字符串 range filter 不被支持）。"
            )

        frames, failed = [pd.DataFrame(res["data"])], []
        for pg in range(2, pages + 1):
            try:
                r = _request(dict(base, pageSize=str(PAGE_SIZE), pageNumber=str(pg)))
                frames.append(pd.DataFrame(r["result"]["data"]))
            except Exception:                                             # noqa: BLE001
                failed.append(pg)
        if failed:
            raise RuntimeError(f"{end_date} 有失败页 {failed} ⇒ 拒绝返回残缺数据")

        df = pd.concat(frames, ignore_index=True)
        if len(df) != expect:
            raise RuntimeError(f"{end_date} 行数不符: 拿到 {len(df)} / 应为 {expect}")
        return self._to_orm(df)

    def _to_orm(self, df: pd.DataFrame) -> pd.DataFrame:
        cm = self.cfg["colmap"]
        missing = [c for c in cm if c not in df.columns]
        if missing:
            raise RuntimeError(f"{self.report} 源缺少预期列: {missing}")
        out = df[list(cm)].rename(columns=cm).copy()

        # 公告日 = datetime（可观测时点）；publish_date 与它恒等，loader 会断言
        if "publish_date" not in out.columns:
            out["publish_date"] = pd.NaT
        out["datetime"] = out["publish_date"]
        out["report_date"] = pd.to_datetime(out["report_date"])
        out["publish_date"] = pd.to_datetime(out["publish_date"])
        out["datetime"] = pd.to_datetime(out["datetime"])

        for c in ("volume", "free_amount", "amount", "volume_change",
                  "volume_change_percent", "holder_rank", "free_holdnum_ratio"):
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors="coerce")
        for c in ("code", "name", "nature", "holder_newtype", "shares_type",
                  "holder_code", "security_name"):
            if c in out.columns:
                out[c] = out[c].astype(str)
        return out
