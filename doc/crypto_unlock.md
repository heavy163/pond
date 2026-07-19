# 代币解锁数据同步方案：ClickHouse 存储 + CMC 免费 API

## 一、背景

当前 `pond/token_unlock/` 模块已实现从 CoinMarketCap 免费公开 API 获取代币解锁数据，
并通过 `UnlockFilter` 实时过滤需要排除的币安合约。但该方案存在以下问题：

| 问题 | 影响 |
|------|------|
| 无持久化存储 | 每次调用都重新拉取 CMC API |
| 无历史追踪 | 无法做解锁前后的价格行为分析 |
| 无定时同步 | 策略中只能"拉取时检查"，无法从数据库统一查询 |
| 映射无共识 | 解锁代币的 symbol/platform/contract_address 三元组未持久化 |

**目标**：将代币解锁数据持久化到 ClickHouse，每个解锁事件全局唯一一行，
由 `ReplacingMergeTree` 自动去重。模型定义沿用 `FutureOpenInterest` 的
SQLAlchemy ORM 模式，定义在 `kline.py` 中，由 `metadata.create_all()` 统一建表。

## 二、数据源

### 2.1 CMC 免费公开 API

```
GET https://api.coinmarketcap.com/data-api/v3/token-unlock/listing
    ?start=1
    &limit=100
    &sort=next_unlocked_date
    &direction=desc
    &enableSmallUnlocks=false
```

**特点**：
- ✅ 完全免费，无需 API key
- ✅ 含 `nextUnlocked` 嵌套结构，含解锁日期、金额、占比
- ⚠️ 单次最多返回 100 条，**不支持翻页**（`start`/`offset` 参数无效）

### 2.2 按窗口天数获取的策略

由于不支持翻页，无法一次性获取全部 406 个代币。但我们的需求并非"全量数据"，
而是"未来 N 天内迫近解锁的代币"。利用 API 的排序特性：

| API 参数 | 效果 |
|----------|------|
| `sort=next_unlocked_date, direction=asc` | 解锁日期从近到远排列 |
| `enableSmallUnlocks=false` | 大额解锁 (≥1%) |
| `enableSmallUnlocks=true` | 小额解锁 (<1%) |

**策略：2 次 API 调用（asc big + asc small），客户端按天数窗口过滤。**

```python
# pond/token_unlock/client.py
def fetch_upcoming_by_window(self, window_days: int = 90) -> list[TokenUnlockEntry]:
    cutoff = datetime.now(timezone.utc) + timedelta(days=window_days)
    param_combos = [
        ("next_unlocked_date", "asc", False),  # 大额解锁
        ("next_unlocked_date", "asc", True),   # 小额解锁
    ]
    # 2 次 API 调用，去重后过滤窗口内的记录
```

**优势**：
- API 调用从 4 次降为 **2 次**
- 只拉取关心的窗口内数据，逻辑更直观
- 配合每日同步，窗口外的记录下次自然补上

**补充说明**：如果窗口较大（如 90 天），API 返回的 100 条全部在窗口内，
则 2 次调用即可覆盖窗口内的全部解锁。如果窗口内记录数超过 100，
`asc` 排序保证我们拿到的是**最近的那些**——远的那些下次同步自动推进。

### 2.3 备选数据源评估

| 数据源 | 结论 | 费用 | 覆盖 | 解锁日程 | 原因 |
|--------|------|------|------|---------|------|
| **CMC 免费 API** | ✅ **当前方案** | 免费 | ~200 条（2 次调用） | ✅ 下次解锁 | 唯一免费可用源，已满足风控需求 |
| **Tokenomist API** | ✅ **最佳付费替代** | $249-449/月 | 1500+ 代币 | ✅ 完整事件线 + 分配明细 | 行业标准，Coinbase/Grayscale/Pantera 在用。有免费试用（50 代币） |
| **CMC Pro API** | ❌ 无解锁数据 | $29-699/月 | — | ❌ | Pro API 没有解锁端点，不提供免费公开 API 之外的数据 |
| **CoinGecko Pro** | ❌ 无解锁日程 | $129+/月 | 按币 | ❌ 仅当前锁仓余额 | `/supply_breakdown` 只显示非流通地址，无解锁时间表 |
| **The Graph 链上** | ⚠️ 工程量大 | 免费 | 按项目 | ✅（有 subgraph 时） | 需逐项目解析锁仓合约，无聚合视图 |
| **CryptoRank** | ❌ 不可用 | — | — | — | 页面已下线 |
| **Etherscan** | ❌ 不实用 | 免费 | 按合约 | ❌ 仅当前余额 | 需预先知道锁仓合约地址，无日程数据 |

### 2.4 升级路径建议

| 阶段 | 方案 | 理由 |
|------|------|------|
| **短期（当前）** | CMC 免费 API | 免费，2 次调用覆盖窗口内数据，配合每日同步 |
| **中期（$249/月预算）** | Tokenomit API（Standard Plan） | 完整解锁事件线、分配分类、1500+ 代币覆盖 |
| **长期（补充）** | The Graph 链上监控 | 验证实际解锁行为与 API 数据是否一致 |

## 三、ClickHouse 表设计

### 3.1 设计原则

| 原则 | 说明 |
|------|------|
| **不继承 TsTable** | 直接继承 `Base`（`from pond.clickhouse import Base`），避免 `datetime` 强制主键 |
| **SQLAlchemy ORM 模式** | 定义模型类，与 `FutureOpenInterest` 并列在 `kline.py` 中 |
| **自动建表** | `metadata.create_all()` 统一创建，无需手动执行 DDL |
| **ReplacingMergeTree** | 按去重 key 自动合并，同 key 保留 `synced_at` 最大者 |
| **去重 key = (symbol, platform, contract_address, next_unlock_time)** | 四个字段唯一确定一个解锁事件 |
| **按月分区** | `partition_by=toYYYYMM(next_unlock_time)`，与现有模型一致 |
| **非快照模式** | 不按天存多份，每个解锁事件全局只有一行 |
| **密等写入** | 同一天多次运行 sync，ReplacingMergeTree 自动覆盖旧值 |

### 3.2 `TokenUnlock` 模型（在 `kline.py` 中定义）

参考 `FutureOpenInterest` 的 SQLAlchemy 模式，但**不继承 `TsTable`**，
继承 `Base` 以完全控制主键和排序列。

```python
# ===== pond/clickhouse/kline.py 中新增 =====

from pond.clickhouse import Base


class TokenUnlock(Base):
    """
    代币解锁信息表

    每个解锁事件全局唯一一行，由 ReplacingMergeTree 按
    (symbol, platform, contract_address, next_unlock_time) 四字段去重。

    不继承 TsTable，因为 TsTable 强制 datetime 为主键和排序列，
    而此表需要自定义 ORDER BY 以支持按解锁事件去重。
    """

    __tablename__ = "token_unlock"

    symbol              = Column(types.String,    comment="CMC 代币符号 (如 ZRO)", primary_key=True)
    platform            = Column(types.String,    comment="区块链平台 (如 ethereum)", primary_key=True)
    contract_address    = Column(types.String,    comment="合约地址 (原生币填 0x0)", primary_key=True)
    next_unlock_time    = Column(types.DateTime64, comment="下次解锁时间 (UTC)", primary_key=True)

    slug                = Column(types.String,    comment="CMC slug (如 layerzero)")
    crypto_id           = Column(types.Int64,     comment="CMC cryptoId")
    name                = Column(types.String,    comment="项目全称")

    total_unlocked_pct  = Column(types.Float64,   comment="已解锁占总供应量 %")
    next_unlock_amount  = Column(types.Float64,   comment="下次解锁数量")
    next_unlock_amount_usd = Column(types.Float64, comment="下次解锁价值 USD")
    next_unlock_pct     = Column(types.Float64,   comment="下次解锁占锁仓 %")

    circulating_supply  = Column(types.Float64,   comment="当前流通量")
    price               = Column(types.Float64,   comment="当前价格 USD")
    market_cap          = Column(types.Float64,   comment="当前市值 USD")

    binance_code        = Column(types.String,    comment="币安合约代码 (如 ZROUSDT，无映射则为空)")

    synced_at           = Column(types.DateTime64, comment="本次同步时间 (UTC)")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(next_unlock_time),
            order_by=(symbol, platform, contract_address, next_unlock_time),
            primary_key=(symbol, platform, contract_address, next_unlock_time),
            version=synced_at,
        ),
    )
```

**与 FutureOpenInterest 的对比**：

| 维度 | `FutureOpenInterest` | `TokenUnlock`（本表） |
|------|---------------------|----------------------|
| 父类 | `TsTable` | `Base` |
| 主键 | `(datetime, code)` | `(symbol, platform, contract_address, next_unlock_time)` |
| ORDER BY | `(datetime, code)` | `(symbol, platform, contract_address, next_unlock_time)` |
| 分区 | `toYYYYMM(datetime)` | `toYYYYMM(next_unlock_time)` |
| 引擎 | `ReplacingMergeTree` | `ReplacingMergeTree(version=synced_at)` |
| `synced_at` | 无版本列 | 作为版本列用于去重决胜 |
| 建表方式 | `metadata.create_all()` | 同左 |

### 3.3 自动建表

无需手动执行 DDL。模型定义在 `kline.py` 后（被导入即可），
`ClickHouseManager.__init__()` 中的 `metadata.create_all(self.engine)` 自动创建：

```python
# manager.py — 已有逻辑，无需修改
class ClickHouseManager:
    def __init__(self, db_uri, ...):
        self.engine = create_engine(db_uri)
        metadata.create_all(self.engine)
```

需要确保 `kline.py` 在 `ClickHouseManager` 初始化前被导入。
当前 `helper.py` 已 `from pond.clickhouse.kline import ...`，自动满足。

### 3.4 数据写入——复用 `save_dataframe()`

`ClickHouseManager` 已有 `save_dataframe(table_name, df)` 方法，无需新增：

```python
self.clickhouse.save_dataframe("token_unlock", df)
```

### 3.5 查询示例

```sql
-- 查询所有即将在大市值币安合约上发生的大额解锁
SELECT binance_code, symbol, next_unlock_time, next_unlock_pct,
       next_unlock_amount_usd, market_cap
FROM token_unlock FINAL
WHERE next_unlock_time >= now()
  AND next_unlock_time <= now() + INTERVAL 14 DAY
  AND next_unlock_pct >= 1.0
  AND binance_code != ''
  AND market_cap > 10000000
ORDER BY next_unlock_amount_usd DESC;

-- 查询某个币的解锁日历
SELECT next_unlock_time, next_unlock_amount, next_unlock_pct, synced_at
FROM token_unlock FINAL
WHERE symbol = 'ZRO'
ORDER BY next_unlock_time;
```

**注意**：`FINAL` 确保 ReplacingMergeTree 返回合并后的结果。
大数据量时可先用 `GROUP BY` 聚合替代 `FINAL`。

## 四、Discriminator 设计（引用 crypto_supply.md）

### 4.1 核心问题

CMC 上同一个 symbol 可能对应多个不同链上的项目（如 `PEPE` 有 ETH 上的 Pepe、
PEPE Chain、Wall Street Pepe 等）。仅用 `baseAsset` 做映射 key 是不够的。

### 4.2 消歧依赖

`platform` / `contract_address` 由 `CMCMarketDataClient`（Pro API + 持久缓存）
维护映射时确定，`TokenUnlock` 表直接保存解析后的三字段，不单独存 discriminator。

```python
# helper.py — FuturesHelper._resolve_platform()
def _resolve_platform(self, symbol: str) -> tuple[str, str]:
    mapping = self.cmc_client.get_cached_mapping(symbol)
    if mapping is None:
        return ("unknown", "0x0")
    return (mapping.get("chain", "unknown") or "unknown",
            mapping.get("contract_address", "0x0") or "0x0")
```

**退化策略**（无 Pro API key）：

```
platform = "unknown"
contract_address = "0x0"
```

此时同一 symbol 的所有变体被归为同一条，至少不会产生数据膨胀。

### 4.3 为什么不需要独立 discriminator 列

方案不需要 `discriminator` 列，因为：

| 问题 | 说明 |
|------|------|
| 去重 key 已经包含三字段 | `(symbol, platform, contract_address, next_unlock_time)` 已唯一确定 |
| discriminator 是派生值 | `symbol::platform::contract_address` 是前 3 个 PK 列的拼接，存为独立列是冗余的 |
| 一致性风险 | 若 PK 字段被更新而 discriminator 未同步，会引入不一致 |
| 查询可随时拼接 | `concat(symbol, '::', platform, '::', contract_address)` 即可 |

## 五、同步流程

### 5.1 整体架构

```
┌─ sync_token_unlock(window_days=90) ────────────────────┐
│                                                            │
│  ① 模型已在 kline.py 中定义，metadata.create_all() 自动   │
│     创建表（幂等，不会重复创建已存在的表）                  │
│                                                            │
│  ② 2 次 API 调用 + 窗口过滤                               │
│     CMCUnlockClient.fetch_upcoming_by_window(90)           │
│     → 2 次调用 (asc big + asc small)，按窗口截断          │
│                                                            │
│  ③ 获取 Binance 合约列表（复用 UnlockFilter）             │
│     uf._get_binance_futures_symbols() → ~665 个 USDT 永续  │
│                                                            │
│  ④ 建立 symbol → binance_code 映射（复用 UnlockFilter）   │
│     uf._build_symbol_map() 多级匹配                        │
│                                                            │
│  ⑤ 构建 ClickHouse 行                                     │
│     ├─ 从 cmc_mapping_cache 解析 platform/contract_address │
│     └─ 组装 DataFrame → save_dataframe("token_unlock")     │
│                                                            │
│  ⑥ 批量写入 ClickHouse                                    │
│     clickhouse.save_dataframe("token_unlock", df)          │
│     ※ ReplacingMergeTree(version=synced_at) 自动去重      │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### 5.2 密等写入

`ReplacingMergeTree(version=synced_at)` 按版本列自动去重：

| 场景 | synced_at | 结果 |
|------|-----------|------|
| 第一次写入 | T1 | 插入新行 |
| 同天第二次写入 | T2 > T1 | 覆盖旧行 |
| CMC 更新了解锁数据 | T3 > T2 | 金额/占比等字段自动更新 |
| 次日再次同步 | T4 > T3 | 最新数据覆盖旧值 |

**无需检查"今天是否已同步"**，每次直接 INSERT，
ReplacingMergeTree 保证幂等。

```python
def __sync_token_unlock(self, window_days, signal, res_dict):
    tid = threading.current_thread().ident
    res_dict[tid] = False
    signal = signal or datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)

    # 1. 按窗口获取数据（2 次 API 调用，自动去重 + 窗口过滤）
    client = CMCUnlockClient()
    entries = client.fetch_upcoming_by_window(window_days=window_days)
    if not entries:
        logger.warning("token_unlock: no data from CMC")
        res_dict[tid] = True
        return

    # 2. 获取币安合约 + 建立映射（复用 UnlockFilter）
    uf = UnlockFilter()
    binance_symbols = uf._get_binance_futures_symbols()
    binance_base_set = set(binance_symbols)
    cmc_to_binance = uf._build_symbol_map(entries, binance_base_set)

    # 3. 组装 ClickHouse 行
    rows = []
    for entry in entries:
        if not entry.next_unlock:
            continue
        platform, contract_addr = self._resolve_platform(entry.symbol)
        rows.append({
            "symbol": entry.symbol,
            "platform": platform,
            "contract_address": contract_addr,
            "next_unlock_time": entry.next_unlock.date,
            "slug": entry.slug,
            "crypto_id": entry.crypto_id,
            "name": entry.name,
            "total_unlocked_pct": entry.total_unlocked_pct,
            "next_unlock_amount": entry.next_unlock.token_amount,
            "next_unlock_amount_usd": entry.next_unlock.token_amount_usd,
            "next_unlock_pct": entry.next_unlock.token_amount_pct,
            "circulating_supply": entry.circulating_supply,
            "price": entry.price or 0.0,
            "market_cap": entry.market_cap or 0.0,
            "binance_code": cmc_to_binance.get(entry.symbol, ""),
            "synced_at": signal,
        })

    # 4. 批量写入（复用 save_dataframe，一行完成）
    if rows:
        df = pd.DataFrame(rows)
        self.clickhouse.save_dataframe("token_unlock", df)
        logger.info(f"token_unlock: saved {len(rows)} rows")

    res_dict[tid] = True
```

### 5.3 sync 函数签名

```python
def sync_token_unlock(
    self,
    window_days: int = 90,
    signal: datetime | None = None,
) -> bool:
    """同步代币解锁数据到 ClickHouse

    幂等，可任意多次调用。ReplacingMergeTree 按
    (symbol, platform, contract_address, next_unlock_time) 自动去重，
    保留 synced_at 最大的一行。

    Args:
        window_days: 未来窗口天数，只同步此窗口内的解锁。默认 90 天。
        signal: 同步时间戳。
    """
```

### 5.4 `sync()` 入口集成

```python
elif what == "token_unlock":
    self.__sync_token_unlock(90, signal, res_dict)
```

## 六、数据流与缓存设计

### 6.1 数据分层

```
CMC Free API                          ClickHouse / 缓存
───────────────                       ─────────────────
token-unlock/listing                  token_unlock (主表)
  ├─ symbol, slug, name                 ├─ symbol (ORDER BY ①)
  ├─ totalUnlockedPercentage            ├─ platform (ORDER BY ②)
  ├─ nextUnlocked.*                     ├─ contract_address (ORDER BY ③)
  └─ quotes.*                           └─ next_unlock_time (ORDER BY ④)

CMCMarketDataClient cache               └─ binance_code (过滤)
(cmc_mapping_cache.json)
  └─ symbol → {chain,
       contract_address}
```

### 6.2 映射缓存结构（复用 `cmc_mapping_cache.json`）

```json
{
  "version": 1,
  "built_at": "2026-07-19T10:00:00Z",
  "symbols": {
    "BTC":  { "cmc_id": 1,   "discriminator": "BTC::native::0x0",                           "chain": null,               "contract_address": null },
    "PEPE": { "cmc_id": 24482, "discriminator": "PEPE::ethereum::0x6982508145454Ce325dDbE47a25d4ec3d2311933", "chain": "ethereum", "contract_address": "0x6982..." },
    "ZRO":  { "cmc_id": 24482, "discriminator": "ZRO::ethereum::0x6982508145454Ce325dDbE47a25d4ec3d2311933", "chain": "ethereum", "contract_address": "0x6982..." }
  },
  "by_discriminator": {
    "BTC::native::0x0": "BTC",
    "PEPE::ethereum::0x6982...": "PEPE"
  },
  "unresolved": ["SOMETHING"]
}
```

### 6.3 退化策略总结

| 资源不可用 | 影响 | 行为 |
|-----------|------|------|
| CMC Pro API key | `platform="unknown"`, `contract_address="0x0"` | 消歧减弱，去重不破 |
| CMC 免费 API 超时 | 跳过本轮同步 | 幂等，下次重试 |
| Binance exchangeInfo | `binance_code=""` | 数据照常写入，后续可补充 |
| ClickHouse 不可用 | 同步失败 | 下次重试 |

## 七、与现有系统的集成

### 7.1 修改文件清单

| 文件 | 改动 |
|------|------|
| `pond/clickhouse/kline.py` | **新增** `TokenUnlock` 模型类（继承 `Base`） |
| `pond/clickhouse/helper.py` | **新增** `__sync_token_unlock()` + `sync_token_unlock()` + `_resolve_platform()` + `get_token_unlock_exclusions()`；**修改** `sync()` 增加 `"token_unlock"` 分支；**新增** import `CMCUnlockClient, UnlockFilter` |
| `pond/token_unlock/client.py` | **重写** `fetch_all()` 为 `fetch_upcoming_by_window(window_days)` — 2 次 API 调用 + 窗口过滤 |
| `pond/token_unlock/binance_filter.py` | **优化** `_get_unlocks()` 支持 `window_days` 参数，减少不必要的数据拉取 |
| `pond/token_unlock/__init__.py` | 无需修改 |

### 7.2 模型注册（自动完成）

模型定义在 `kline.py` 中、继承 `Base` 后，自动注册到 `metadata`。
`metadata.create_all()` 遍历所有注册的模型自动建表。
当前 `helper.py` 已 `from pond.clickhouse.kline import ...`，导入链完整。

### 7.3 策略层查询——`get_token_unlock_exclusions()`

```python
def get_token_unlock_exclusions(
    self,
    window_days: int = 14,
    min_unlock_pct: float = 1.0,
    min_market_cap: float = 10_000_000,
) -> pd.DataFrame:
    """从 ClickHouse 获取窗口内大额解锁事件

    Returns:
        DataFrame，包含 binance_code, symbol, next_unlock_time,
        next_unlock_pct, next_unlock_amount_usd, market_cap 等字段。
        调用方自行决定取哪些字段。
    """
    sql = """
        SELECT *
        FROM token_unlock FINAL
        WHERE next_unlock_time >= now()
          AND next_unlock_time <= now() + INTERVAL :window DAY
          AND next_unlock_pct >= :min_pct
          AND binance_code != ''
          AND market_cap >= :min_mcap
        ORDER BY next_unlock_amount_usd DESC
    """
    return self.clickhouse.native_sql_read_table(sql, {...})

# 调用方视角
df = helper.get_token_unlock_exclusions()
excluded = df["binance_code"].tolist() if df is not None and not df.empty else []
```

### 7.4 与现有 UnlockFilter 的关系

| 组件 | 职责 | 数据源 | 延迟 |
|------|------|--------|------|
| `UnlockFilter`（现有） | 运行时实时过滤 | CMC API 直查 | ~2s |
| `token_unlock` 表 | 持久化 + 历史分析 | ClickHouse | ~分钟级 |
| `get_token_unlock_exclusions()` | 策略定期刷新排除列表 | ClickHouse | ~ms |

三者互补：`UnlockFilter` 用于策略初始化时的即时排除，
`token_unlock` 表用于跨 session 共享和历史回溯，
`get_token_unlock_exclusions()` 是策略层的一行调用封装。

## 八、边界情况与异常处理

| 场景 | 处理方式 |
|------|---------|
| CMC API 空列表 | 跳过写入，WARNING，返回 True（非阻塞） |
| CMC API 超时/429 | 重试 3 次，仍失败 → 返回 False |
| Binance exchangeInfo 不可用 | 所有 `binance_code=""`，不影响写入 |
| symbol 未匹配 Binance | `binance_code=""`，该行仍写入供后续分析 |
| 无 Pro API key | `platform="unknown"`, `contract="0x0"` |
| 同一天多次调用 | ReplacingMergeTree 自动去重 |
| 解锁日期已过期 | 保留在表中，查询时过滤 `>= now()` |
| 无 `next_unlock` | 跳过该行（去重 key 不完整） |
| 新上币 | `_build_symbol_map()` 未匹配 → `binance_code=""` |
| 窗口内记录超过 100 条 | `asc` 排序保证取到最近的那些，远的在下次同步中推进 |

## 九、已知问题与修复方案

### 9.1 platform 全是 unknown

**根因**：`cmc_mapping_cache.json` 只在 `__sync_futures_info()` 中填充（仅含 Binance 永续合约标的），
解锁代币列表中的大部分不在其中，`_resolve_platform()` 找不到映射。

**方案**：`__sync_token_unlock()` 在遍历 entries 前补一次 resolve_mapping：

```python
need_resolve = {entry.symbol for entry in entries if entry.next_unlock}
cached = {s for s in need_resolve if self.cmc_client.get_cached_mapping(s)}
uncached = list(need_resolve - cached)
if uncached:
    logger.info(f"Resolving {len(uncached)} uncached unlock symbols...")
    new_mappings = self.cmc_client.resolve_mapping(uncached)
    self.cmc_client._update_cache(new_mappings, uncached)
```

Pro API 不可用时退化回 `"unknown"`，不影响核心流程。

### 9.2 binance_code 映射不全

**根因**：`_build_symbol_map()` 只按 CMC symbol 精确匹配。解锁代币的 CMC symbol 常与币安 baseAsset
不一致（如 `DBR` → `DEBRIDGE`、`KULA` → 不匹配）。

**方案**：在 `_build_symbol_map()` 中增加两个 fallback 匹配层：

| 优先级 | 匹配方式 | 示例 |
|--------|---------|------|
| ① | CMC symbol → 币安 baseAsset（现有） | `PEPE` → `1000PEPE` |
| ② | CMC slug 大写下划线转空 | `debridge` → `DEBRIDGE` → 匹配 `DBR` |
| ③ | CMC name 首字母缩写 | `deBridge` → `DBR` |

```python
# 在 _build_symbol_map 现有匹配后增加：

# 6) slug 大写去连字符
slug_clean = cmc_slug.upper().replace("-", "").replace(" ", "")
if slug_clean in binance_bases:
    cmap[cmc_symbol] = f"{slug_clean}USDT"
    continue

# 7) name 首字母缩写
acronym = "".join(w[0] for w in entry.name.upper().split() if w)
if acronym in binance_bases and acronym != cmc_symbol:
    cmap[cmc_symbol] = f"{acronym}USDT"
    continue
```

这样不依赖手动维护映射表，覆盖大部分命名差异。

### 9.3 market_cap 为 0

**不修复**。CMC 免费 API 对部分代币不返回市值。查询时用 `WHERE market_cap >= :min_mcap` 过滤即可，
不是代码缺陷。

---

## 十、实现状态

### ✅ 已完成

- [x] `kline.py` 定义 `TokenUnlock`（继承 `Base`，不继承 `TsTable`；无冗余 discriminator 列；有 partition_by）
- [x] `client.py` 实现 `fetch_upcoming_by_window(window_days)` — 2 次 API 调用 + 窗口过滤，替代旧的 4 次全量拉取
- [x] `binance_filter.py` 优化 `_get_unlocks()` 支持 `window_days` 参数
- [x] `helper.py` 实现 `__sync_token_unlock()` — `fetch_upcoming_by_window()` → 映射 → `save_dataframe()`
- [x] `helper.py` 实现 `_resolve_platform()` — 从 cmc_mapping_cache 解析 platform/contract_address
- [x] `helper.py` 实现 `get_token_unlock_exclusions()` — 策略层一键查询排除列表
- [x] `sync()` 入口注册 `"token_unlock"` 分支
- [x] 复用 `UnlockFilter`, `save_dataframe()` 等现有组件

### P1（后续优化）

- [ ] `__sync_token_unlock()` 预填充 resolve_mapping（修复 §9.1）
- [ ] `_build_symbol_map()` 增加 slug/name fallback（修复 §9.2）
- [ ] 定时同步（cron / scheduler）
- [ ] Tokenomist API 集成评估
