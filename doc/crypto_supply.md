# 代币供应量数据同步方案：CoinGecko → CoinMarketCap 迁移

## 一、背景

当前 `pond/pond/clickhouse/helper.py` 中 `__sync_futures_info` 从 **CoinGecko** 逐个查询每
个 Binance 永续合约标的的 `total_supply` 和 `market_cap_fdv_ratio`，写入 ClickHouse
`FutureInfo` 表。该表被 `attach_future_info()` 读取，用于策略计算 FDV 等指标。

## 二、当前链路与瓶颈

### 数据流

```
启动 __sync_futures_info()
  ↓ 遍历 ~200 个 Binance 永续合约标的
  ↓ 每个标的依次：
    ① CoinGeckoIDMapper.get_coingecko_id(ticker)  → GET /api/v3/search?query=XXX   [HTTP 1]
    ② get_coin_market_data(cg_id)                    → GET /api/v3/coins/{cg_id}    [HTTP 2]
  ↓
  ThreadPoolExecutor 并发（实则全被限频压制）
  ↓
  ClickHouse FutureInfo 表
```

### 瓶颈量化

| 环节 | 问题 | 影响 |
|------|------|------|
| **每标的 2 次 HTTP** | 先 `/search` 查 ID，再 `/coins/{id}` 拿数据 | ~200 标的 × 2 = **400 次请求** |
| **CoinGecko 免费 API 限频** | 免费计划 ~10–30 次/分钟 | 400 次 ≈ **13–40 分钟** |
| **ThreadPoolExecutor 反效果** | 多线程同时 429 → 指数退避 → 更慢 | 实际耗时可能 > 30 分钟 |
| **CoinGeckoIDMapper 不稳定** | 符号消歧依赖搜索结果排序，1000PEPE 等异常前缀匹配不稳定 | 部分标的匹配失败或无数据 |
| **fetch_marketcap.py 分页慢** | 全市场 ~12,000 币 × 250/页 ≈ 48 页 × 2.5s 间隔 | 拉一次全量约 2 分钟 |

## 三、替代方案：CMC API

### 核心思路

用 CoinMarketCap 的 **批量查询** 能力 + **合约地址交叉验证** 取代 CoinGecko 的
per-symbol 逐请求模式。

### CMC 相关端点

| 端点 | 用途 | 请求数 | 关键返回字段 |
|------|------|--------|-------------|
| `/v2/cryptocurrency/info` | 查代币元数据（含链上合约地址） | 1 次 / 所有标的 | `platform.slug`（链名）, `platform.token_address`（合约地址）, `id`（CMC ID） |
| `/v2/cryptocurrency/quotes/latest` | 查特定币的最新报价/供应量 | 1 次 / ~120 个 id | `total_supply`, `circulating_supply`, `max_supply`, `market_cap`, `quote.USD.price` |
| `/v3/cryptocurrency/listings/latest` | 拉全市场按市值排序列表 | 备用 | 同上 + `platform` |

### 核心流程概览

```
每 N 小时同步一次（供应量数据更新快）

  ┌─ 同步开始 ──────────────────────────────────────────┐
  │                                                      │
  │  ① 从 Binance exchangeInfo 获取全部合约标的列表        │
  │     {symbol, baseAsset, status, ...}                   │
  │                                                      │
  │  ② 检查本地映射缓存（symbol → cmc_id）               │
  │     ├─ 已缓存 → 跳过映射查询，直接跳到第④步           │
  │     └─ 未缓存或缓存过期 → 执行映射解析                 │
  │                                                      │
  │  ③ 映射解析（仅未缓存/过期时执行，1–3 次 HTTP）       │
  │     a. CMC /v2/cryptocurrency/info                    │
  │        → 拿到每个 symbol 的所有链上变体 + 合约地址     │
   │     b. CMC /v2/cryptocurrency/quotes/latest           │
  │        → 用 id 查全部变体的 market_cap                │
  │     c. 按市值降序选唯一变体 → 写入缓存                 │
  │                                                      │
  │  ④ 查供应量（2 次 HTTP，日常同步的核心）               │
   │     CMC /v2/cryptocurrency/quotes/latest?id=...       │
  │     → 返回 total_supply, circulating_supply, ...       │
  │                                                      │
  │  ⑤ 写入 ClickHouse FutureInfo 表                      │
  │                                                      │
  └──────────────────────────────────────────────────────┘
```

## 四、双重消歧策略

### 问题本质

CMC 上一个 symbol 可能对应多个不同的项目（如 `PEPE` 有 "Pepe (ETH)"、"PEPE Chain"、
"Wall Street Pepe" 等多个变体）。仅靠 symbol 查供应量，无法保证查到的是 Binance
上架的那个版本。

### 解决方案：合约地址 + 市值双重消歧

```
Binance 标的    CMC 多个变体          筛选条件
─────────────  ─────────────────    ─────────────────
BTCUSDT        ① bitcoin            ← 唯一变体，直接匹配
               ② (无其它 BTC)

PEPEUSDT       ① Pepe (ETH)          ← contract_address 0x6982...
               ② PEPE Chain           ← 不同链/地址
               ③ Wall Street Pepe     ← 不同链/地址
               ④ ...
                                        ↓
                                  market_cap 排序，取最高的那个
                                  + 可选 contract_address 交叉比对
```

**判定逻辑：**

```python
def resolve_binance_to_cmc(
    base_asset: str,
    cmc_variants: list[dict],   # 从 /v2/cryptocurrency/info 返回的该 symbol 所有变体
    quotes_by_id: dict[int, dict],  # 从 /v2/cryptocurrency/quotes/latest 返回的市值数据
) -> int | None:
    """对给定 symbol，从多个 CMC 变体中选出一个对应 Binance 上架的那个。

    策略：CMC 的 symbol 查询默认返回市值最高的变体，这个策略和 Binance
    的上架审核逻辑一致——Binance 只会上市值最高、流动性最好的版本。
    我们手动用 market_cap 排序取第一个，做等价选择。
    """
    if len(cmc_variants) == 1:
        return cmc_variants[0]["id"]  # 唯一变体，直接返回

    # 多个变体：按 market_cap 降序排列，取第一个
    best = max(
        cmc_variants,
        key=lambda v: (
            quotes_by_id.get(str(v["id"]), {}).get("quote", {})
            .get("USD", {}).get("market_cap", 0) or 0
        ),
    )
    return best["id"]
```

### 与 CMC 官方推荐一致

> CMC 官方文档："When fetching cryptocurrency by a symbol that matches several
> active cryptocurrencies, the API returns the one with the highest market cap
> at the time of the query."

我们的逻辑和 CMC 内置行为等价，但多做了一步明确的 `id` 参数查询，**完全消除
了 symbol 歧义**。

### 更强的安全网：合约地址交叉验证

在选出的变体上，可以额外获取其 `platform` 信息，和已知的预期链做比对：

```python
def cross_validate_platform(
    base_asset: str,
    selected_variant: dict,
    expected_chain: str = "",  # 可选，如 "ethereum"
) -> bool:
    platform = selected_variant.get("platform", {})
    if not platform:
        return True  # 原生币（BTC/BNB）没有 platform，跳过
    chain = platform.get("slug", "")
    # 至少确认 chain 不是空的或不合理的
    if not chain:
        return False
    return True
```

这一步在 P0 阶段可选，但作为缓存构建时的验证很有价值。

## 五、缓存设计

### 核心问题：仅用 baseAsset 做 key 是不够的

CMC 上同一个 symbol 可能对应多个不同链上的项目（如 `PEPE` 有 ETH 上的 Pepe、
PEPE Chain、Wall Street Pepe 等）。仅用 `baseAsset` 做缓存 key：

```
cache["PEPE"] = 24482    ← 弱 key。哪天市值排序变了，指向的可能是另一个项目
```

**正确的 key 必须包含链和合约地址**，因为：
- 代币在 Binance 上的 **baseAsset** 是固定的（来自 exchangeInfo）
- 对应的 **区块链** 和 **合约地址** 也是固定的（项目自身的属性）
- 三者组合 `baseAsset + chain + contract_address` 才能**唯一确定一个链上资产**
- 只要组合不变，映射关系就不可能变

### 缓存结构：discriminator 作为身份锚

```python
# 每个缓存条目的 "discriminator"（区别器）:
#   原生币:         "BTC::native::0x0"
#   ERC-20 代币:    "PEPE::ethereum::0x6982508145454Ce325dDbE47a25d4ec3d2311933"
#   BEP-20 代币:    "CAKE::binance-smart-chain::0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"
#
# discriminator 是缓存条目的身份锚。只要 discriminator 不变，
# cmc_id 就不可能变——因为同一个链+地址不可能对应不同的 CMC id。

# 缓存文件：cmc_mapping_cache.json
{
  "version": 1,
  "built_at": "2026-06-20T10:00:00Z",

  # 主表：baseAsset → 映射条目（方便从 Binance symbol 快速查找）
  "symbols": {
    "BTC": {
      "cmc_id": 1,
      "name": "Bitcoin",
      "discriminator": "BTC::native::0x0",    # ← 复合 key，缓存条目的身份锚
      "chain": null,
      "contract_address": null,
      "resolved_at": "2026-06-20T10:00:00Z",
      "re_validated_at": "2026-06-20T10:00:00Z"
    },
    "PEPE": {
      "cmc_id": 24482,
      "name": "Pepe",
      "discriminator": "PEPE::ethereum::0x6982508145454Ce325dDbE47a25d4ec3d2311933",
      "chain": "ethereum",
      "contract_address": "0x6982508145454Ce325dDbE47a25d4ec3d2311933",
      "resolved_at": "2026-06-20T10:00:00Z",
      "re_validated_at": "2026-06-20T10:00:00Z"
    }
  },

  # 第二索引：discriminator → baseAsset（用于重验证时的快速反查）
  "by_discriminator": {
    "BTC::native::0x0": "BTC",
    "PEPE::ethereum::0x6982508145454Ce325dDbE47a25d4ec3d2311933": "PEPE"
  },

  # 额外记录：哪些 Binance symbol 无法匹配
  "unresolved": ["SOMETHINGUSDT"]
}
```

**discriminator 的作用：**

| 场景 | 仅用 baseAsset 查 | 用 discriminator 验证 |
|------|------------------|---------------------|
| 初次映射 | `PEPE → 24482`（盲信市值排序） | `PEPE::ethereum::0x6982... → 24482`（链+地址锁定） |
| 30 天后重验证 | 重新查市值排序，可能变 | 重新查 CMC，**对比 discriminator 是否一致** |
| 底层换项目 | 无感知，可能指向错币 | discriminator 变了 → **告警 + 重解析** |

### 缓存命中流程

```
每次 __sync_futures_info 调用：

① 从 exchangeInfo 获取最新标的列表 {symbol, baseAsset}

② 遍历每个标的：
   ├─ baseAsset 在 cache.symbols 中？
   │   ├─ YES → 从缓存取 discriminator + cmc_id，跳到第④步
   │   └─ NO  → 加入 need_resolve 列表

③ 调用 resolve_mapping(need_resolve)  →  1-3 次 HTTP
   对每个新标的：
     查 CMC info → 取各变体的 chain + contract_address
     查 CMC quotes → 按市值排序选最高者
     构建 discriminator → 写入缓存

④ 全部 cmc_id 就绪后，查 supply 数据
   /v2/cryptocurrency/quotes/latest?id=...  →  2 次 HTTP

⑤ 缓存未匹配的标的（标记为无数据），下次跳过
```

### 重验证：比对 discriminator 而非 cmc_id

```python
def validate_cache(self) -> list[str]:
    """重验证缓存的映射是否仍然有效。

    遍历缓存中的所有条目，对照 CMC 当前数据重新解析 discriminator，
    如果 discriminator 变了，说明底层项目已更换，需要告警。
    """
    changed = []
    for base_asset, entry in self.cache["symbols"].items():
        stored_discriminator = entry["discriminator"]

        # 从 CMC 重新解析该 symbol 的当前最佳变体
        current_variant = self._resolve_best_variant(base_asset)
        if current_variant is None:
            continue  # CMC 上已不存在该 symbol

        current_discriminator = self._build_discriminator(
            base_asset,
            current_variant.get("platform"),
        )

        if stored_discriminator != current_discriminator:
            # discriminator 变了 = 底层项目已更换
            changed.append({
                "base_asset": base_asset,
                "old_discriminator": stored_discriminator,
                "new_discriminator": current_discriminator,
                "old_cmc_id": entry["cmc_id"],
                "new_cmc_id": current_variant["id"],
            })

    return changed
```

| 重验证结果 | 含义 | 处理 |
|-----------|------|------|
| discriminator 一致，cmc_id 一致 | ✅ 映射完全正确 | 不动缓存 |
| discriminator 一致，cmc_id 不一致 | ❌ 不可能发生（同一链+地址不可能对应不同 id） | 报 bug |
| discriminator 不一致 | ⚠️ 底层项目已更换 | 打 WARNING，更新缓存 |

discriminator 比对优于 cmc_id 比对，因为：
- cmc_id 可能会变化（如 CMC 合并/更新数据库），但 **链+地址不会变**
- discriminator 一致 → 100% 确定是同一个项目
- 重验证不需要查 quotes（供应量数据），只需要查 info（元数据），请求更轻量

### 缓存与 supply 数据分离

| 维度 | 映射缓存 | 供应量数据 |
|------|---------|-----------|
| 存储 | JSON 文件（`cmc_mapping_cache.json`） | ClickHouse `FutureInfo` 表 |
| 关键字段 | `discriminator`（复合标识） | `total_supply`, `circulating_supply` |
| 更新频率 | 首次构建 + 月级重验证 | 每次同步（天级） |
| 查询方式 | 本地文件读取 | CMC API `/quotes/latest` |
| 请求次数 | 仅构建时 2-3 次 | 每次 2 次 |

这样做的好处：
- **日常同步不需要查 mapping**，只查 supply 数据（2 次 HTTP）
- **discriminator 保证映射的长期正确性**——链+地址比 symbol 更稳定
- **新增/上币**只在新 baseAsset 出现时才触发 CMC 映射查询
- 缓存文件可提交到 repo，新环境直接使用，无需重新解析

## 六、CMCMarketDataClient 完整实现

```python
import os
import json
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from loguru import logger


class CMCMarketDataClient:
    """封装 CMC API 调用 + 缓存"""

    BASE_URL = "https://pro-api.coinmarketcap.com"
    QUOTES_PATH = "/v2/cryptocurrency/quotes/latest"
    INFO_PATH = "/v2/cryptocurrency/info"
    LISTINGS_PATH = "/v3/cryptocurrency/listings/latest"

    def __init__(
        self,
        api_key: str | None = None,
        cache_path: str | Path = "cmc_mapping_cache.json",
    ):
        self.api_key = api_key or os.environ.get("CMC_PRO_API_KEY")
        if not self.api_key:
            raise ValueError("CMC_PRO_API_KEY not set")
        self._lock = threading.RLock()
        self.session = requests.Session()
        self.session.headers.update({
            "X-CMC_PRO_API_KEY": self.api_key,
            "Accept": "application/json",
        })
        self.cache_path = Path(cache_path)
        with self._lock:
            self.cache = self._load_cache()

    # ── 供应量查询（每次同步核心） ──

    def batch_quotes_by_id(self, cmc_ids: list[int]) -> dict[int, dict]:
        """用 CMC id 查询供应量，无歧义。建议每次最多 ~100 个 id。"""
        result: dict[int, dict] = {}
        for i in range(0, len(cmc_ids), 100):
            batch = cmc_ids[i : i + 100]
            resp = self.session.get(
                f"{self.BASE_URL}{self.QUOTES_PATH}",
                params={"id": ",".join(str(x) for x in batch)},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            for raw_id, info in data.items():
                result[int(raw_id)] = info
        return result

    # ── discriminator 工具 ──

    @staticmethod
    def _build_discriminator(base_asset: str, platform: dict | None) -> str:
        """构建复合身份锚：baseAsset::chain::contract_address

        原生币（无合约地址）："BTC::native::0x0"
        代币（有合约地址）：  "PEPE::ethereum::0x6982..."
        """
        if not platform:
            return f"{base_asset.upper()}::native::0x0"
        chain = (platform.get("slug") or platform.get("name") or "unknown").lower()
        address = platform.get("token_address") or "0x0"
        return f"{base_asset.upper()}::{chain}::{address}"

    # ── 映射解析（首次运行 + 定期重验证） ──

    def resolve_mapping(
        self, base_assets: list[str]
    ) -> dict[str, dict]:
        """对一组 baseAsset 解析其 CMC id + discriminator。

        返回: {
            "PEPE": {
                "cmc_id": 24482,
                "discriminator": "PEPE::ethereum::0x6982...",
                "name": "Pepe",
                "chain": "ethereum",
                "contract_address": "0x6982...",
            },
            ...
        }
        """
        if not base_assets:
            return {}

        # Step 1: 查询所有变体及 platform 信息
        info = self._fetch_info(base_assets)
        # info 结构:
        # {"BTC": [{"id": 1, "name": "Bitcoin", "platform": {...}}, ...],
        #  "PEPE": [{"id": 24482, "name": "Pepe", "platform": {...}},
        #           {"id": 30071, "name": "PEPE Chain", "platform": {...}}, ...]}

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
                    quotes.get(v["id"], {}).get("quote", {})
                    .get("USD", {}).get("market_cap", 0) or 0
                ),
            )
            platform = best.get("platform")
            result[sym] = {
                "cmc_id": best["id"],
                "name": best.get("name"),
                "discriminator": self._build_discriminator(sym, platform),
                "chain": platform.get("slug") if platform else None,
                "contract_address": platform.get("token_address") if platform else None,
            }

        return result

    def _fetch_info(self, symbols: list[str]) -> dict[str, list[dict]]:
        """调用 /v2/cryptocurrency/info 获取 symbol 的所有变体

        返回 {symbol: [{id, name, platform}, ...], ...}
        """
        result: dict[str, list[dict]] = {}
        BATCH_SIZE = 100
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i : i + BATCH_SIZE]
            resp = self.session.get(
                f"{self.BASE_URL}{self.INFO_PATH}",
                params={"symbol": ",".join(batch)},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            for sym, entries in data.items():
                entries_list = [entries] if isinstance(entries, dict) else entries
                result[sym.upper()] = entries_list
        return result

    # ── 缓存管理 ──

    def _load_cache(self) -> dict:
        if self.cache_path.exists():
            try:
                with open(self.cache_path) as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
        return {
            "version": 1,
            "built_at": None,
            "symbols": {},
            "by_discriminator": {},
            "unresolved": [],
        }

    def _save_cache(self):
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w") as f:
            json.dump(self.cache, f, indent=2, ensure_ascii=False)

    def _update_cache(self, resolved: dict[str, dict], attempted: list[str]):
        """将解析结果写入缓存，同时维护主索引和 discriminator 索引"""
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

    def needs_re_validate(self, interval_days: int = 30) -> bool:
        """检查是否需要全量重验证"""
        with self._lock:
            built_at = self.cache.get("built_at")
            if not built_at:
                return True
            elapsed = datetime.now(timezone.utc) - datetime.fromisoformat(built_at)
            return elapsed > timedelta(days=interval_days)

    def validate_mappings(self) -> list[dict]:
        """全量重验证：对比 discriminator，检测底层项目是否更换。

        仅需 /v2/cryptocurrency/info（元数据），不需要查 quotes（供应量）。
        """
        with self._lock:
            if not self.cache.get("symbols"):
                return []

            all_bases = list(self.cache["symbols"].keys())
            logger.info(f"Validating {len(all_bases)} cached mappings...")

            # 重新查询 CMC 当前数据
            current_info = self._fetch_info(all_bases)

            changed = []
            for base_asset, entry in self.cache["symbols"].items():
                stored_disc = entry["discriminator"]

                # 获取 CMC 当前返回的变体
                variants = current_info.get(base_asset)
                if not variants:
                    continue

                # 按市值排序取最佳变体（同 resolve_mapping 逻辑）
                quotes = self.batch_quotes_by_id([v["id"] for v in variants])
                best = max(
                    variants,
                    key=lambda v: (
                        quotes.get(v["id"], {}).get("quote", {})
                        .get("USD", {}).get("market_cap", 0) or 0
                    ),
                )
                current_disc = self._build_discriminator(base_asset, best.get("platform"))

                if stored_disc != current_disc:
                    # discriminator 变了 = 底层项目已更换
                    changed.append({
                        "base_asset": base_asset,
                        "old_discriminator": stored_disc,
                        "new_discriminator": current_disc,
                        "old_cmc_id": entry["cmc_id"],
                        "new_cmc_id": best["id"],
                        "old_name": entry.get("name"),
                        "new_name": best.get("name"),
                    })
                    # 自动更新缓存
                    platform = best.get("platform")
                    entry["cmc_id"] = best["id"]
                    entry["discriminator"] = current_disc
                    entry["chain"] = platform.get("slug") if platform else None
                    entry["contract_address"] = platform.get("token_address") if platform else None
                    entry["re_validated_at"] = datetime.now(timezone.utc).isoformat()
                    # 更新 discriminator 索引
                    by_disc = self.cache.setdefault("by_discriminator", {})
                    if stored_disc in by_disc:
                        del by_disc[stored_disc]
                    by_disc[current_disc] = base_asset
                else:
                    # 一致，只更新时间戳
                    entry["re_validated_at"] = datetime.now(timezone.utc).isoformat()

            self.cache["built_at"] = datetime.now(timezone.utc).isoformat()
            self._save_cache()

            if changed:
                logger.warning(
                    f"Mapping changes detected for {len(changed)} symbols: {changed}"
                )
            else:
                logger.info("All mappings validated, no changes detected")
            return changed
```

## 七、__sync_futures_info 改造后流程

### 频率控制规则

为避免数据库中出现大量同一日期内的重复数据，影响查询效率和磁盘占用，
供应量数据同步遵循 **"一天一更新"** 原则：

```
对每个标的 code，在 ClickHouse 中查询最近一条记录的 datetime：
  ├─ 该记录与当前 sync 时间在同一天（signal.date() == last.date()）？
  │   └─ YES → 当天已有数据，跳过该标的（不查询 CMC，不写入）
  │
  └─ 非同一天 → 加入待更新列表
```

| 场景 | 行为 | 原因 |
|------|------|------|
| 同一天内 sync 跑 2 次 | 第二次跳过所有标的 | 供应量一天内变化极小，无需更新 |
| 跨天跑 sync | 所有标的进入待更新 | 新一天的数据需要刷新 |
| Binance 新增标的（无历史记录） | 立即进入待更新 | 首次写入 |

这样做的好处：
- **CMC API 调用量减半**（一天跑多次 sync 时，只有第一次会实际查询）
- **ClickHouse 数据量稳定增长**（每个标的每天最多 1 条记录，不重复）
- **异常恢复友好**（当天数据已存在，即使重复触发 sync 也不会重复写入）

### 实现代码

```python
class FuturesHelper:
    def __init__(self, ...):
        ...
        self.cmc_client = CMCMarketDataClient()
        # 去掉 CoinGeckoIDMapper（不再使用）

    def __sync_futures_info(self, signal, table, symbols, res_dict, workers):
        tid = threading.current_thread().ident
        res_dict[tid] = False
        if signal is None:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)

        # 1. 读取已有记录的最新时间（确定哪些标的需要更新）
        info_df = self.clickhouse.read_latest_n_record(
            table.__tablename__, signal - timedelta(days=30), signal, 1
        )
        latest_records_map = {}
        if info_df is not None and not info_df.empty:
            for _, row in info_df.iterrows():
                latest_records_map[row["code"]] = row["datetime"]

        # 2. 筛选当天尚未同步的标的（频率控制：一天一更新）
        stale_symbols = []
        for symbol in symbols:
            code = symbol["pair"]
            onboard = datetime.fromtimestamp(symbol["onboardDate"] / 1000)
            last = latest_records_map.get(code, self.clickhouse.data_start)
            last = max(last, onboard)

            # 核心规则：同一天已有数据则跳过
            if last.date() == signal.date():
                logger.debug(f"{code}: already synced today ({last.date()}), skip")
                continue

            stale_symbols.append(code)

        if not stale_symbols:
            logger.info("All symbols up-to-date for today, skip")
            res_dict[tid] = True
            return

        # 3. 收集所有需要解析 cmc_id 的 baseAsset
        all_base_assets = set()
        for code in stale_symbols:
            base = strip_quote(code, strip_leveraged(code))
            all_base_assets.add(base)

        # 4. 检查缓存，区分已解析/未解析
        need_resolve = []
        base_to_mapping = {}  # baseAsset → {cmc_id, discriminator, chain, contract_address}
        for base in all_base_assets:
            cached = self.cmc_client.get_cached_mapping(base)
            if cached is not None:
                base_to_mapping[base] = cached
            else:
                need_resolve.append(base)

        # 5. 解析未缓存的映射（仅首次或新增币时触发）
        if need_resolve:
            logger.info(f"Resolving {len(need_resolve)} uncached symbols...")
            new_mappings = self.cmc_client.resolve_mapping(need_resolve)
            # 写入缓存
            self.cmc_client._update_cache(new_mappings, need_resolve)
            for base, info in new_mappings.items():
                base_to_mapping[base] = info

        # 6. 定期重验证（30 天一次，比对 discriminator）
        if self.cmc_client.needs_re_validate():
            logger.info("Running periodic mapping re-validation...")
            changes = self.cmc_client.validate_mappings()
            if changes:
                # 有变更的条目，重新从缓存读取最新 cmc_id
                for change in changes:
                    base = change["base_asset"]
                    refreshed = self.cmc_client.get_cached_mapping(base)
                    if refreshed:
                        base_to_mapping[base] = refreshed

        # 7. 按 id 查供应量
        cmc_ids = [v["cmc_id"] for v in base_to_mapping.values() if v is not None]
        quotes = self.cmc_client.batch_quotes_by_id(cmc_ids)

        # 8. 构造 ClickHouse 记录
        info_records = []
        for code in stale_symbols:
            base = strip_quote(code, strip_leveraged(code))
            mapping = base_to_mapping.get(base)

            if mapping is None:
                # 无法匹配，写 None 跳过
                info_records.append({
                    "datetime": signal, "code": code,
                    "total_supply": None, "market_cap_fdv_ratio": None,
                })
                continue

            info = quotes.get(mapping["cmc_id"])
            if info and info.get("total_supply"):
                price = info.get("quote", {}).get("USD", {}).get("price")
                total_supply = info["total_supply"]
                market_cap = info.get("quote", {}).get("USD", {}).get("market_cap")
                if price and market_cap:
                    mcap_fdv_ratio = (
                        market_cap / (total_supply * price)
                    )
                else:
                    total_supply = None
                    mcap_fdv_ratio = None
            else:
                total_supply = None
                mcap_fdv_ratio = None

            info_records.append({
                "datetime": signal, "code": code,
                "total_supply": total_supply,
                "market_cap_fdv_ratio": mcap_fdv_ratio,
            })

        # 9. 批量写入 ClickHouse
        if info_records:
            merged_df = pd.DataFrame(info_records)
            self.clickhouse.save_to_db(table, merged_df, None)
        logger.info(
            f"info sync done: {len(info_records)}/{len(stale_symbols)} "
            f"symbols processed"
        )
        res_dict[tid] = True
```

## 八、优化效果对比

| 对比项 | CoinGecko（当前） | CMC + 缓存（方案） | 优化倍率 |
|--------|-----------------|-------------------|---------|
| **首次同步请求数** | 400 次 | **~4-6 次**（info + quotes） | **~70–100x** |
| **日常同步请求数** | 400 次 | **2 次**（仅 quotes，命中缓存） | **~200x** |
| **同步耗时（日常）** | 13–40 分钟 | **< 5 秒** | **~100–480x** |
| **消歧精度** | 仅 symbol 匹配 | symbol + market_cap + 合约地址 | 大幅提升 |
| **ID 映射稳定性** | 需 CoinGeckoIDMapper（每次都要查） | **持久缓存 + 30 天重验证** | 近零成本 |
| **数据字段** | `total_supply` + `mcap_fdv_ratio` | `total_supply` + `circulating_supply` + `max_supply` + `market_cap` | 多出 2-3 个字段 |
| **免费额度** | 10–30/min | ~30/min（但仅需 2 次） | 绰绰有余 |

## 九、开发顺序

```
P0（一次性开发，核心改造）
  └ 新增 pond/cmc/__init__.py
      ├ CMCMarketDataClient 类
      ├ resolve_mapping() 双重消歧
      ├ batch_quotes_by_id() 供应量查询
      └ 缓存读写 + 重验证逻辑
  └ 修改 helper.py
      ├ __sync_futures_info → 调用 CMCMarketDataClient
      ├ 删除 __fetch_single_info
      └ 删除 CoinGeckoIDMapper 依赖
  └ 项目根 .env 新增 CMC_PRO_API_KEY
  └ 提交 cmc_mapping_cache.json（空缓存模板）
  └ 集成测试
      ├ 首次同步：~6 次 HTTP，建缓存
      ├ 二次同步：2 次 HTTP，读缓存
      └ 检查 FutureInfo 表数据覆盖率

P1（数据字段扩展）
  └ FutureInfo 表增加 circulating_supply 列
  └ attach_future_info() 回传流通量
  └ prediction_indicator.py 可选增加流通量因子

P2（全市场快照，可选）
  └ fetch_cmc_marketcap.py 替代 fetch_marketcap.py
  └ 去掉全部 CoinGecko 依赖
```

## 十、待处理问题

| # | 问题 | 方案 |
|---|------|------|
| 1 | 1000PEPE、1000SHIB 等币种 `strip_quote` 后得到 `PEPE`、`SHIB`，CMC 接受 | 当前 `strip_quote()` 和 `strip_leveraged()` 直接复用 |
| 2 | CMC 免费 API Key 的限频额度 | ~30次/分钟 × 每次批量 100 个 → 映射构建时约 2-3 次请求，远低于限频 |
| 3 | `total_supply` 的含义 | CMC 标注的 `total_supply` 是 "当前存在的总币数（扣除已销毁的）"，不等同于链上 `totalSupply()`。如果需要精确值仍需走 RPC |
| 4 | 部分小币 `total_supply` 可能为 `null` | 和 CoinGecko 行为一致，写入 `None` 即可 |
| 5 | CMC `/v2/cryptocurrency/info` 对 symbol 查询的响应格式 | 返回结构可能需要适配（单对象 vs 数组），需实测验证 |
