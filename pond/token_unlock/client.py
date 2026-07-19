"""
CoinMarketCap 公开 API 客户端 — 代币解锁数据

使用免费公开 API（无需 API key）获取代币解锁时间表。

数据源: GET https://api.coinmarketcap.com/data-api/v3/token-unlock/listing

响应结构:
    {
        "data": {
            "tokenUnlockList": [
                {
                    "cryptoId": int,
                    "symbol": "ZRO",
                    "slug": "layerzero",
                    "name": "LayerZero",
                    "circulatingSupply": 3.54e8,
                    "totalSupply": 1e9,
                    "maxSupply": 1e9,
                    "totalUnlockedPercentage": 64.13,
                    "nextUnlocked": {
                        "tokenAmount": 3.261e7,       # 解锁数量
                        "tokenAmountUsd": 2.646e7,     # 解锁价值 USD
                        "tokenAmountPercentage": 3.26, # 占总锁仓 %
                        "date": 1784505600000          # 解锁时间戳(ms)
                    },
                    "quotes": [{"price": 0.8114, ...}]
                },
                ...
            ],
            "totalCount": "406"
        }
    }
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

import requests
from loguru import logger


# ═══════════════════════════════════════════════════════════════════
#  Data Models
# ═══════════════════════════════════════════════════════════════════


@dataclass
class NextUnlock:
    """下次解锁详情"""

    token_amount: float
    token_amount_usd: float
    token_amount_pct: float  # 占总锁仓百分比
    date: datetime  # 解锁时间 (UTC)

    @classmethod
    def from_dict(cls, d: dict) -> "NextUnlock":
        return cls(
            token_amount=float(d.get("tokenAmount", 0)),
            token_amount_usd=float(d.get("tokenAmountUsd", 0)),
            token_amount_pct=float(d.get("tokenAmountPercentage", 0)),
            date=datetime.fromtimestamp(d["date"] / 1000, tz=timezone.utc),
        )


@dataclass
class TokenUnlockEntry:
    """单个代币的解锁信息"""

    crypto_id: int
    symbol: str  # CMC symbol (大写)
    slug: str  # URL path name
    name: str
    circulating_supply: float
    total_supply: float | None
    max_supply: float | None
    total_unlocked_pct: float  # 已解锁百分比
    next_unlock: NextUnlock | None
    price: float | None
    market_cap: float | None

    @classmethod
    def from_cmc_item(cls, item: dict) -> "TokenUnlockEntry":
        quotes = item.get("quotes", [{}])
        quote = quotes[0] if quotes else {}
        nu_raw = item.get("nextUnlocked")

        return cls(
            crypto_id=int(item["cryptoId"]),
            symbol=item["symbol"],
            slug=item.get("slug", ""),
            name=item.get("name", ""),
            circulating_supply=float(item.get("circulatingSupply", 0) or 0),
            total_supply=(
                float(item["totalSupply"])
                if item.get("totalSupply") is not None
                else None
            ),
            max_supply=(
                float(item["maxSupply"]) if item.get("maxSupply") is not None else None
            ),
            total_unlocked_pct=float(item.get("totalUnlockedPercentage", 0)),
            next_unlock=NextUnlock.from_dict(nu_raw) if nu_raw else None,
            price=float(quote["price"]) if quote.get("price") is not None else None,
            market_cap=float(quote["marketCap"])
            if quote.get("marketCap") is not None
            else None,
        )


# ═══════════════════════════════════════════════════════════════════
#  Client
# ═══════════════════════════════════════════════════════════════════


class CMCUnlockClient:
    """
    CoinMarketCap 代币解锁公开 API 客户端

    完全免费，无需 API key。直接从 CMC 前端使用的内部 API 获取数据。

    用法:
        client = CMCUnlockClient()
        entries = client.fetch_upcoming(limit=100)
        for e in entries:
            print(e.symbol, e.next_unlock.date, e.next_unlock.token_amount_pct)
    """

    BASE_URL = "https://api.coinmarketcap.com/data-api/v3"
    UNLOCK_LISTING_PATH = "/token-unlock/listing"

    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 3,
        user_agent: str | None = None,
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": (
                    user_agent
                    or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            }
        )

    def fetch_upcoming(
        self,
        limit: int = 100,
        sort: str = "next_unlocked_date",
        direction: str = "desc",
        enable_small_unlocks: bool = False,
    ) -> list[TokenUnlockEntry]:
        """获取即将解锁的代币列表（单次 API 调用）

        Args:
            limit: 返回条数上限 (实际 max 100)
            sort: 排序字段
                  - "next_unlocked_date": 下次解锁日期
                  - "total_unlocked": 已解锁占比
                  - "token_unlocked": 通证解锁进度
                  - "cryptoId": CMC 内部 ID
            direction: "asc" | "desc"
                 注意: 同一 sort 下 asc 和 desc 返回的是**完全不重叠**的两批数据。
            enable_small_unlocks: 是否包含小额解锁 (<1% of circ supply)

        Returns:
            List[TokenUnlockEntry] (最多 100 条)
        """
        params = {
            "start": 1,
            "limit": min(limit, 100),
            "sort": sort,
            "direction": direction,
            "enableSmallUnlocks": str(enable_small_unlocks).lower(),
        }

        data = self._request("GET", self.UNLOCK_LISTING_PATH, params=params)
        raw_list = data.get("data", {}).get("tokenUnlockList", [])

        return [TokenUnlockEntry.from_cmc_item(item) for item in raw_list]

    def fetch_upcoming_by_window(self, window_days: int = 90) -> list[TokenUnlockEntry]:
        """获取指定天数内即将解锁的代币

        CMC API 不支持翻页（start/offset 无效），单次最多返回 100 条。
        但 direction=asc 的排序行为是"解锁日期从近到远"，配合
        enableSmallUnlocks=true/false 两种参数，2 次调用即可覆盖窗口内全部解锁。

        Args:
            window_days: 未来窗口天数。默认 90 天，覆盖大部分迫近解锁。

        Returns:
            List[TokenUnlockEntry] (去重后，仅含窗口内的记录)
        """
        cutoff = datetime.now(timezone.utc) + timedelta(days=window_days)
        param_combos = [
            ("next_unlocked_date", "asc", False),  # 大额解锁 (≥1%)
            ("next_unlocked_date", "asc", True),  # 小额解锁 (<1%)
        ]

        seen: set[tuple] = set()
        all_entries: list[TokenUnlockEntry] = []

        for sort, direction, small in param_combos:
            try:
                entries = self.fetch_upcoming(
                    sort=sort,
                    direction=direction,
                    enable_small_unlocks=small,
                )
                for e in entries:
                    dedup_key = (e.crypto_id, e.symbol, e.slug, e.name)
                    if dedup_key not in seen:
                        seen.add(dedup_key)
                        all_entries.append(e)
            except RuntimeError as exc:
                logger.warning(
                    f"fetch_upcoming_by_window sub-call failed "
                    f"(sort={sort}, dir={direction}, small={small}): {exc}"
                )
                continue

        in_window = [
            e for e in all_entries if e.next_unlock and e.next_unlock.date <= cutoff
        ]
        logger.info(
            f"fetch_upcoming_by_window(window_days={window_days}): "
            f"{len(in_window)}/{len(all_entries)} unique tokens in window"
        )
        return in_window

    fetch_all = fetch_upcoming_by_window

    DETAIL_PATH = "/cryptocurrency/detail"

    def fetch_detail(self, crypto_id: int) -> TokenUnlockEntry | None:
        """通过 CMC detail 端点查询单个代币的解锁详情

        CMC 的 /token-unlock/listing 批量端点不返回所有代币（如 BANK 不在列表中），
        但 /cryptocurrency/detail?id=X 端点包含 tokenUnlockLatest 字段。
        此方法作为 listing 的补充，用于补查批量接口遗漏的代币。

        注意：CMC CDN 对 detail 端点有缓存，需要时间戳参数保证拿到最新数据。

        返回:
            TokenUnlockEntry 或 None（无解锁数据时）
        """
        ts = int(time.time() * 1000)
        data = self._request(
            "GET", self.DETAIL_PATH, params={"id": crypto_id, "_t": ts}
        )
        d = data.get("data", {})
        unlock = d.get("tokenUnlockLatest")
        if not unlock or not unlock.get("nextUnlocked"):
            return None
        quotes = d.get("quotes", [{}])
        quote = quotes[0] if quotes else {}
        return TokenUnlockEntry(
            crypto_id=crypto_id,
            symbol=d.get("symbol", ""),
            slug=d.get("slug", ""),
            name=d.get("name", ""),
            circulating_supply=float(d.get("circulatingSupply", 0) or 0),
            total_supply=(
                float(d["totalSupply"]) if d.get("totalSupply") is not None else None
            ),
            max_supply=(
                float(d["maxSupply"]) if d.get("maxSupply") is not None else None
            ),
            total_unlocked_pct=float(unlock.get("totalUnlockedPercentage", 0)),
            next_unlock=NextUnlock(
                token_amount=float(unlock["nextUnlocked"]["tokenAmount"]),
                token_amount_usd=float(unlock["nextUnlocked"]["tokenAmountUsd"]),
                token_amount_pct=float(unlock["nextUnlocked"]["tokenAmountPercentage"]),
                date=datetime.fromtimestamp(
                    unlock["nextUnlocked"]["date"] / 1000, tz=timezone.utc
                ),
            ),
            price=float(quote["price"]) if quote.get("price") is not None else None,
            market_cap=float(quote["marketCap"])
            if quote.get("marketCap") is not None
            else None,
        )

    # ═════════════════════════════════════════════════════════════
    #  内部方法
    # ═════════════════════════════════════════════════════════════

    def _request(self, method: str, path: str, params: dict | None = None) -> dict:
        """带重试的请求封装"""
        url = f"{self.BASE_URL}{path}"
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.request(
                    method,
                    url,
                    params=params,
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                last_error = e
                status = getattr(e, "response", None)
                status_code = status.status_code if status else "N/A"

                if status_code == 429:
                    wait = min(2**attempt * 2, 30)
                    logger.warning(
                        f"CMC rate limited (attempt {attempt}), " f"waiting {wait}s..."
                    )
                    time.sleep(wait)
                    continue

                logger.debug(
                    f"CMC request failed (attempt {attempt}/{self.max_retries}): "
                    f"{method} {path} -> {status_code}"
                )
                if attempt < self.max_retries:
                    time.sleep(1)

        raise RuntimeError(
            f"CMC unlock API failed after {self.max_retries} retries: {last_error}"
        )


# ═══════════════════════════════════════════════════════════════════
#  快捷函数
# ═══════════════════════════════════════════════════════════════════


def get_upcoming_unlocks(
    limit: int = 100,
    min_unlock_pct: float = 0.0,
    window_days: int | None = None,
) -> list[TokenUnlockEntry]:
    """快速获取即将解锁的代币（一行调用）

    Args:
        limit: 最大返回数量
        min_unlock_pct: 最小解锁占比过滤 (e.g. 1.0 = 1%)
        window_days: 只看未来 N 天内解锁 (None = 不限)

    Returns:
        List[TokenUnlockEntry]
    """
    client = CMCUnlockClient()
    entries = client.fetch_upcoming(limit=limit)

    now = datetime.now(timezone.utc)

    filtered = []
    for e in entries:
        if e.next_unlock is None:
            continue
        if e.next_unlock.token_amount_pct < min_unlock_pct:
            continue
        if window_days is not None:
            cutoff = now + timedelta(days=window_days)
            if e.next_unlock.date > cutoff:
                continue
        filtered.append(e)

    return filtered
