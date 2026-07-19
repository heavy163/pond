"""
CoinMarketCap API 客户端：批量查询代币供应量 + 合约地址消歧 + 持久缓存

功能：
  - batch_quotes_by_id: 用 CMC id 无歧义查供应量（日常同步核心，2 次 HTTP）
  - resolve_mapping:    首次解析 Binance symbol → CMC id（双重消歧）
  - validate_mappings:  30 天重验证（比对 discriminator）
  - 持久缓存 cmc_mapping_cache.json，避免重复解析

数据源：https://coinmarketcap.com/api/documentation/v1/
"""

import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests
from loguru import logger


class CMCMarketDataClient:
    """封装 CMC API 调用 + 持久缓存"""

    BASE_URL = "https://pro-api.coinmarketcap.com"
    QUOTES_PATH = "/v2/cryptocurrency/quotes/latest"
    INFO_PATH = "/v2/cryptocurrency/info"
    LISTINGS_PATH = "/v3/cryptocurrency/listings/latest"
    MAX_SYMBOLS_PER_REQUEST = 100
    CACHE_VERSION = 1
    RE_VALIDATE_INTERVAL_DAYS = 30

    def __init__(
        self,
        api_key: str | None = None,
        cache_path: str | Path = "cmc_mapping_cache.json",
    ):
        self.api_key = api_key or os.environ.get("CMC_PRO_API_KEY")
        if not self.api_key:
            raise ValueError(
                "CMC_PRO_API_KEY not set. Pass api_key= or set env var."
            )
        self._lock = threading.RLock()
        self.session = requests.Session()
        self.session.headers.update({
            "X-CMC_PRO_API_KEY": self.api_key,
            "Accept": "application/json",
        })
        self.cache_path = Path(cache_path)
        with self._lock:
            self.cache = self._load_cache()

    # ════════════════════════════════════════════════════════════════
    #  公开 API：供应量查询（每次同步核心，一天一次）
    # ════════════════════════════════════════════════════════════════

    def batch_quotes_by_id(self, cmc_ids: list[int]) -> dict[int, dict]:
        """用 CMC id 批量查最新报价 + 供应量，无歧义。

        每次最多 ~100 个 id（CMC 限制），自动分页。
        返回 {cmc_id: {total_supply, circulating_supply, max_supply,
                       market_cap, quote: {USD: {price, ...}}}, ...}
        """
        result: dict[int, dict] = {}
        for i in range(0, len(cmc_ids), self.MAX_SYMBOLS_PER_REQUEST):
            batch = cmc_ids[i: i + self.MAX_SYMBOLS_PER_REQUEST]
            resp = self._request(
                "GET",
                self.QUOTES_PATH,
                params={"id": ",".join(str(x) for x in batch)},
            )
            data = resp.get("data", {})
            for raw_id, info in data.items():
                result[int(raw_id)] = info
        return result

    # ════════════════════════════════════════════════════════════════
    #  公开 API：映射解析（仅首次 / 新增标的时触发）
    # ════════════════════════════════════════════════════════════════

    def resolve_mapping(self, base_assets: list[str]) -> dict[str, dict]:
        """对一组 baseAsset 解析其 CMC id + discriminator。

       双重消歧：
           1. /v2/cryptocurrency/info → 获取每个 symbol 的所有链上变体
           2. /v2/cryptocurrency/quotes/latest → 按 market_cap 选最高者

        返回:
          {
            "PEPE": {
              "cmc_id": 24482,
              "discriminator": "PEPE::ethereum::0x6982...",
              "name": "Pepe",
              "chain": "ethereum",
              "contract_address": "0x6982...",
            },
            ...
          }

          resolve_mapping 本身不写缓存，由调用方在拿到结果后调用 _update_cache。
          这样调用方可以决定是否全部成功后再写入。
        """
        if not base_assets:
            return {}

        # Step 1: 获取所有变体 + platform 信息
        info = self._fetch_info(base_assets)

        # 收集所有候选 id
        all_ids: set[int] = set()
        for sym, variants in info.items():
            for v in variants:
                all_ids.add(v["id"])

        # Step 2: 查市值排序
        quotes = self.batch_quotes_by_id(list(all_ids))

        # Step 3: 每个 symbol 选市值最高的变体，构建 discriminator
        result: dict[str, dict] = {}
        for sym, variants in info.items():
            best = max(
                variants,
                key=lambda v: (
                    quotes.get(v["id"], {})
                    .get("quote", {})
                    .get("USD", {})
                    .get("market_cap", 0)
                    or 0
                ),
            )
            platform = best.get("platform")
            result[sym] = {
                "cmc_id": best["id"],
                "name": best.get("name"),
                "discriminator": self._build_discriminator(sym, platform),
                "chain": platform.get("slug") if platform else None,
                "contract_address": (
                    platform.get("token_address") if platform else None
                ),
            }

        return result

    # ════════════════════════════════════════════════════════════════
    #  公开 API：重验证（30 天一次，比对 discriminator）
    # ════════════════════════════════════════════════════════════════

    def validate_mappings(self) -> list[dict]:
        """全量重验证：对比 discriminator，检测底层项目是否更换。

        仅需 /v2/cryptocurrency/info（元数据），不需要查 quotes（供应量）。
        如果发现 discriminator 不一致，自动更新缓存并返回变更列表。

        返回:
          [{"base_asset", "old_discriminator", "new_discriminator",
            "old_cmc_id", "new_cmc_id", "old_name", "new_name"}, ...]
        """
        with self._lock:
            if not self.cache.get("symbols"):
                return []

            all_bases = list(self.cache["symbols"].keys())
            logger.info(f"Validating {len(all_bases)} cached mappings...")

            current_info = self._fetch_info(all_bases)
            changed = []

            for base_asset, entry in self.cache["symbols"].items():
                stored_disc = entry["discriminator"]
                variants = current_info.get(base_asset)
                if not variants:
                    continue

                # 按市值取最佳变体
                quotes = self.batch_quotes_by_id([v["id"] for v in variants])
                best = max(
                    variants,
                    key=lambda v: (
                        quotes.get(v["id"], {})
                        .get("quote", {})
                        .get("USD", {})
                        .get("market_cap", 0)
                        or 0
                    ),
                )
                current_disc = self._build_discriminator(
                    base_asset, best.get("platform")
                )

                if stored_disc != current_disc:
                    changed.append({
                        "base_asset": base_asset,
                        "old_discriminator": stored_disc,
                        "new_discriminator": current_disc,
                        "old_cmc_id": entry["cmc_id"],
                        "new_cmc_id": best["id"],
                        "old_name": entry.get("name"),
                        "new_name": best.get("name"),
                    })
                    # 自动更新
                    platform = best.get("platform")
                    entry["cmc_id"] = best["id"]
                    entry["name"] = best.get("name")
                    entry["discriminator"] = current_disc
                    entry["chain"] = platform.get("slug") if platform else None
                    entry["contract_address"] = (
                        platform.get("token_address") if platform else None
                    )
                    entry["re_validated_at"] = datetime.now(timezone.utc).isoformat()

                    by_disc = self.cache.setdefault("by_discriminator", {})
                    if stored_disc in by_disc:
                        del by_disc[stored_disc]
                    by_disc[current_disc] = base_asset
                else:
                    entry["re_validated_at"] = datetime.now(timezone.utc).isoformat()

            self.cache["built_at"] = datetime.now(timezone.utc).isoformat()
            self._save_cache()

            if changed:
                logger.warning(
                    f"Mapping changes detected for {len(changed)} symbols: "
                    f"{[c['base_asset'] for c in changed]}"
                )
            else:
                logger.info("All mappings validated, no changes detected")
            return changed

    # ════════════════════════════════════════════════════════════════
    #  缓存管理
    # ════════════════════════════════════════════════════════════════

    @staticmethod
    def _build_discriminator(base_asset: str, platform: dict | None) -> str:
        """构建复合身份锚：baseAsset::chain::contract_address

        原生币（无合约地址）："BTC::native::0x0"
        代币（有合约地址）：  "PEPE::ethereum::0x6982..."
        """
        if not platform:
            return f"{base_asset.upper()}::native::0x0"
        chain = (
            platform.get("slug") or platform.get("name") or "unknown"
        ).lower()
        address = platform.get("token_address") or "0x0"
        return f"{base_asset.upper()}::{chain}::{address}"

    def get_cached_mapping(self, base_asset: str) -> dict | None:
        """从缓存中获取映射条目（含 cmc_id + discriminator）

        返回 None 表示未缓存或已知不可解析。
        """
        with self._lock:
            entry = self.cache.get("symbols", {}).get(base_asset.upper())
            if entry:
                return {
                    "cmc_id": entry["cmc_id"],
                    "discriminator": entry["discriminator"],
                    "chain": entry.get("chain"),
                    "contract_address": entry.get("contract_address"),
                }
            if base_asset.upper() in self.cache.get("unresolved", []):
                return None
            return None

    def needs_re_validate(self, interval_days: int | None = None) -> bool:
        """检查是否需要全量重验证"""
        with self._lock:
            if interval_days is None:
                interval_days = self.RE_VALIDATE_INTERVAL_DAYS
            built_at = self.cache.get("built_at")
            if not built_at:
                return True
            elapsed = datetime.now(timezone.utc) - datetime.fromisoformat(built_at)
            return elapsed > timedelta(days=interval_days)

    def _update_cache(self, resolved: dict[str, dict], attempted: list[str]):
        """将解析结果写入缓存，同时维护主索引和 discriminator 索引

        调用方保证 resolved 中的所有条目都来自 resolve_mapping() 的有效结果。
        attempted 用于记录哪些 symbol 尝试过但未匹配。
        """
        with self._lock:
            now = datetime.now(timezone.utc).isoformat()
            symbols = self.cache.setdefault("symbols", {})
            by_disc = self.cache.setdefault("by_discriminator", {})

            for sym, info in resolved.items():
                symbols[sym] = {
                    "cmc_id": info["cmc_id"],
                    "name": info.get("name"),
                    "discriminator": info["discriminator"],
                    "chain": info.get("chain"),
                    "contract_address": info.get("contract_address"),
                    "resolved_at": now,
                    "re_validated_at": now,
                }
                by_disc[info["discriminator"]] = sym

            unresolved_set = set(self.cache.get("unresolved", []))
            matched = set(resolved.keys())
            for sym in attempted:
                if sym not in matched:
                    unresolved_set.add(sym)
            self.cache["unresolved"] = sorted(unresolved_set)

            self.cache["built_at"] = now
            self._save_cache()

    # ════════════════════════════════════════════════════════════════
    #  内部方法
    # ════════════════════════════════════════════════════════════════

    def _request(
        self, method: str, path: str, params: dict | None = None
    ) -> dict:
        """带重试和 429 退避的请求封装"""
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            resp = self.session.request(
                method,
                f"{self.BASE_URL}{path}",
                params=params,
                timeout=30,
            )
            if resp.status_code == 429:
                wait = min(2 ** attempt * 5, 120)
                logger.warning(
                    f"CMC 429 rate limited (attempt {attempt}/{max_retries}), "
                    f"waiting {wait}s..."
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(
            f"CMC request failed after {max_retries} retries: "
            f"{method} {path}"
        )

    def _fetch_info(self, symbols: list[str]) -> dict[str, list[dict]]:
        """调用 /v2/cryptocurrency/info 获取 symbol 的所有变体

        返回 {symbol: [{id, name, platform}, ...], ...}

        部分无效 symbol 会导致 CMC 返回 400，此时自动二分降级重试。
        """
        return self._fetch_info_batch(symbols)

    def _fetch_info_batch(
        self, symbols: list[str]
    ) -> dict[str, list[dict]]:
        if not symbols:
            return {}
        result: dict[str, list[dict]] = {}
        for i in range(0, len(symbols), self.MAX_SYMBOLS_PER_REQUEST):
            batch = symbols[i: i + self.MAX_SYMBOLS_PER_REQUEST]
            try:
                resp = self._request(
                    "GET",
                    self.INFO_PATH,
                    params={"symbol": ",".join(batch)},
                )
            except requests.HTTPError as e:
                resp_obj = getattr(e, "response", None)
                if resp_obj is not None and resp_obj.status_code == 400:
                    if len(batch) == 1:
                        logger.warning(
                            f"Symbol {batch[0]} not found on CMC"
                        )
                        continue
                    mid = len(batch) // 2
                    time.sleep(1)
                    left = self._fetch_info_batch(batch[:mid])
                    right = self._fetch_info_batch(batch[mid:])
                    result.update(left)
                    result.update(right)
                    continue
                raise
            data = resp.get("data", {})
            for sym, entries in data.items():
                entries_list = (
                    [entries] if isinstance(entries, dict) else entries
                )
                result[sym.upper()] = entries_list
        return result

    def _load_cache(self) -> dict:
        if self.cache_path.exists():
            try:
                with open(self.cache_path) as f:
                    data = json.load(f)
                if data.get("version") == self.CACHE_VERSION:
                    return data
                logger.info(
                    f"Cache version mismatch, rebuilding: "
                    f"stored={data.get('version')}, current={self.CACHE_VERSION}"
                )
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
        return {
            "version": self.CACHE_VERSION,
            "built_at": None,
            "symbols": {},
            "by_discriminator": {},
            "unresolved": [],
        }

    def _save_cache(self):
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.cache_path.with_suffix(".json.tmp")
        with open(tmp, "w") as f:
            json.dump(self.cache, f, indent=2, ensure_ascii=False)
        tmp.replace(self.cache_path)
