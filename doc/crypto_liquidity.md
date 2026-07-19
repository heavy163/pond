# 代币流动性数据同步方案：ClickHouse 存储 + DEX Screener 免费 API

## 一、背景

当前策略已通过 `token_unlock` 表掌握代币解锁事件。解锁事件发生时，代币的
DEX 流动性直接影响价格冲击程度和滑点——流动性越差，同等解锁量的价格影响越大。

**目标**：将币安 USDT 永续合约标的的 DEX 流动性数据持久化到 ClickHouse，
每个代币-链-DEX 池全局唯一一行，由 `ReplacingMergeTree` 自动去重。
模型定义与 `TokenUnlock` 并列在 `kline.py` 中。

| 问题 | 影响 |
|------|------|
| 无流动性数据 | 无法评估解锁事件的价格冲击风险 |
| 无跨 DEX 聚合 | 同一代币在 Uniswap / PancakeSwap / Orca 上流动性分布未知 |
| 无定时刷新 | 流动性是动态指标，需定期更新 |

## 二、数据源

### 2.1 DEX Screener 免费 API

```
# 按代币地址搜索交易对
GET https://api.dexscreener.com/token-pairs/v1/{chainId}/{tokenAddress}

# 按关键词搜索
GET https://api.dexscreener.com/latest/dex/search?q={symbol}
```

**特点**：
- ✅ **完全免费，无需 API key**
- ✅ 返回每个交易对的 `liquidity.usd`、`volume.h24`、`priceUsd`、`fdv`
- ✅ 覆盖 40+ 链，100+ DEX（Uniswap V2/V3/V4、PancakeSwap、SushiSwap、Orca、Raydium 等）
- ⚠️ 限速 60 req/min

**响应结构**：

```json
{
  "schemaVersion": "1.0.0",
  "pairs": [
    {
      "chainId": "ethereum",
      "dexId": "uniswap_v3",
      "pairAddress": "0x...",
      "baseToken": { "address": "0x...", "name": "ZRO", "symbol": "ZRO" },
      "quoteToken": { "address": "0x...", "name": "WETH", "symbol": "WETH" },
      "priceUsd": "0.8114",
      "priceNative": "0.00042",
      "liquidity": { "usd": 2850000, "base": 3500000, "quote": 1200 },
      "volume": { "h24": 1250000, "h6": 320000, "h1": 55000, "m5": 5000 },
      "fdv": 287250000,
      "pairCreatedAt": 1689600000000
    }
  ]
}
```

### 2.2 备选数据源评估

| 数据源 | 费用 | 池级流动性 | 覆盖 | 结论 |
|--------|------|-----------|------|------|
| **DEX Screener** | **免费，无 key** | ✅ `liquidity.usd` | 40 链，100+ DEX | ⭐ **主数据源** |
| GeckoTerminal | 免费 100c/m | ✅ `reserve_in_usd` | 200 链 | 备选，免费限额低 |
| DeFiLlama | 免费 | ❌ 仅协议 TVL | 200+ 链 | 不适合逐池 |
| Birdeye | $39+/月 | ✅ 多 DEX 聚合 | 10 链 | 付费不必要 |
| Uniswap Subgraph | 免费 | ✅ 完整链上数据 | 单 DEX | 太复杂，跨 DEX 需多 subgraph |

**结论**：DEX Screener 是此场景下最合适的数据源——免费、零配置、按池返回流动性数据。

## 三、ClickHouse 表设计

### 3.1 设计原则

| 原则 | 说明 |
|------|------|
| **不继承 TsTable** | 直接继承 `Base`，避免 `datetime` 强制主键 |
| **SQLAlchemy ORM 模式** | 定义在 `kline.py` 中，与 `TokenUnlock` 并列 |
| **自动建表** | `metadata.create_all()` 统一创建 |
| **ReplacingMergeTree** | 按去重 key 自动合并，同 key 保留 `synced_at` 最大者 |
| **去重 key = (symbol, chain, dex_id, pair_address)** | 四个字段唯一确定一个 DEX 流动性池 |
| **按月分区** | `partition_by=toYYYYMM(synced_at)` |
| **快照模式** | 每天存一份全量快照，ReplacingMergeTree 覆盖旧值 |

### 3.2 `TokenLiquidity` 模型（在 `kline.py` 中定义）

```python
class TokenLiquidity(Base):
    """
    代币 DEX 流动性快照表

    每个代币在各链各 DEX 上的流动性池全局唯一一行，
    由 ReplacingMergeTree 按 (symbol, chain, dex_id, pair_address) 四字段去重。
    """

    __tablename__ = "token_liquidity"

    # ── 去重 key ──
    symbol          = Column(types.String,    comment="代币符号 (如 ZRO)", primary_key=True)
    chain           = Column(types.String,    comment="链标识 (ethereum, bsc, solana)", primary_key=True)
    dex_id          = Column(types.String,    comment="DEX 标识 (uniswap_v3, pancakeswap)", primary_key=True)
    pair_address    = Column(types.String,    comment="池合约地址", primary_key=True)

    # ── 池元数据 ──
    base_token      = Column(types.String,    comment="base token 地址")
    base_token_name = Column(types.String,    comment="base token 名称")
    quote_token     = Column(types.String,    comment="quote token 地址")
    quote_token_name = Column(types.String,   comment="quote token 名称")
    pair_created_at = Column(types.DateTime64, comment="池创建时间")

    # ── 流动性数据 ──
    liquidity_usd   = Column(types.Float64,   comment="池总流动性 USD")
    liquidity_base  = Column(types.Float64,   comment="base token 流动性数量")
    liquidity_quote = Column(types.Float64,   comment="quote token 流动性数量")

    # ── 交易数据 ──
    volume_h24      = Column(types.Float64,   comment="24h 交易量 USD")
    volume_h6       = Column(types.Float64,   comment="6h 交易量 USD")
    volume_h1       = Column(types.Float64,   comment="1h 交易量 USD")
    txns_buys_h24   = Column(types.Int64,     comment="24h 买单数")
    txns_sells_h24  = Column(types.Int64,     comment="24h 卖单数")

    # ── 价格数据 ──
    price_usd       = Column(types.Float64,   comment="当前价格 USD")
    price_native    = Column(types.Float64,   comment="当前价格 (原生代币)")
    fdv             = Column(types.Float64,   comment="全稀释估值 USD")
    market_cap      = Column(types.Float64,   comment="市值 USD")

    # ── 价格变化 ──
    price_change_h24 = Column(types.Float64,  comment="24h 价格变化 %")
    price_change_h6  = Column(types.Float64,  comment="6h 价格变化 %")
    price_change_h1  = Column(types.Float64,  comment="1h 价格变化 %")
    price_change_m5  = Column(types.Float64,  comment="5m 价格变化 %")

    # ── 同步审计 ──
    synced_at       = Column(types.DateTime64, comment="本次同步时间 (UTC)")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(synced_at),
            order_by=(symbol, chain, dex_id, pair_address),
            primary_key=(symbol, chain, dex_id, pair_address),
            version=synced_at,
        ),
    )
```

### 3.3 与 TokenUnlock 的对比

| 维度 | `TokenUnlock` | `TokenLiquidity` |
|------|--------------|-----------------|
| 父类 | `Base` | `Base` |
| 主键 | `(symbol, platform, contract_address, next_unlock_time)` | `(symbol, chain, dex_id, pair_address)` |
| 分区 | `toYYYYMM(next_unlock_time)` | `toYYYYMM(synced_at)` |
| 引擎 | `ReplacingMergeTree(version=synced_at)` | 同左 |
| 数据量 | 每币 1 行 | 每币 N 行（N 个 DEX 池） |
| 更新频率 | 每日 | 每日 |

## 四、同步流程

### 4.1 整体架构

```
┌─ sync_token_liquidity() ──────────────────────────────┐
│                                                            │
│  ① 从 Binance exchangeInfo 获取 USDT 永续合约列表         │
│     提取 baseAsset → 约 665 个代币                       │
│                                                            │
│  ② 对每个 baseAsset，调用 DEX Screener search API         │
│     GET /latest/dex/search?q={symbol}                     │
│     → 返回该代币在所有链/DEX 上的交易对                    │
│                                                            │
│  ③ 解析响应，按 liquidity.usd 降序排序                    │
│     保留前 N 个流动性最高的池                              │
│                                                            │
│  ④ 组装 DataFrame → INSERT INTO token_liquidity           │
│     ※ ReplacingMergeTree(version=synced_at) 自动去重      │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### 4.2 API 调用策略

DEX Screener 限速 60 req/min。665 个代币一次同步需要约 11 分钟（665 ÷ 60）。

**优化策略**：只查询已上币安且未被 `UnlockFilter` 标记的"安全"代币，
或在首次同步后只更新变动较大的新币。

```python
def __sync_token_liquidity(self, signal, res_dict):
    tid = threading.current_thread().ident
    res_dict[tid] = False
    signal = signal or datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)

    # 1. 获取币安永续合约 baseAsset 列表
    symbols = self.get_perpetual_symbols(signal)
    base_assets = list({self._strip_quote(s["pair"]) for s in symbols})
    logger.info(f"token_liquidity: {len(base_assets)} base assets to check")

    # 2. 逐个查询 DEX Screener（限速 60 req/min）
    rows = []
    session = requests.Session()
    for i, base in enumerate(base_assets):
        try:
            resp = session.get(
                f"https://api.dexscreener.com/latest/dex/search?q={base}",
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.debug(f"token_liquidity: search failed for {base}: {e}")
            continue

        pairs = data.get("pairs", [])
        if not pairs:
            continue

        # 按流动性降序，取前 5 个池
        pairs.sort(key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0), reverse=True)
        for pair in pairs[:5]:
            rows.append({
                "symbol": base.upper(),
                "chain": pair.get("chainId", ""),
                "dex_id": pair.get("dexId", ""),
                "pair_address": pair.get("pairAddress", ""),
                "base_token": pair.get("baseToken", {}).get("address", ""),
                "base_token_name": pair.get("baseToken", {}).get("name", ""),
                "quote_token": pair.get("quoteToken", {}).get("address", ""),
                "quote_token_name": pair.get("quoteToken", {}).get("name", ""),
                "pair_created_at": datetime.fromtimestamp(
                    pair.get("pairCreatedAt", 0) / 1000, tz=dtm.timezone.utc
                ) if pair.get("pairCreatedAt") else None,
                "liquidity_usd": float(pair.get("liquidity", {}).get("usd", 0) or 0),
                "liquidity_base": float(pair.get("liquidity", {}).get("base", 0) or 0),
                "liquidity_quote": float(pair.get("liquidity", {}).get("quote", 0) or 0),
                "volume_h24": float(pair.get("volume", {}).get("h24", 0) or 0),
                "volume_h6": float(pair.get("volume", {}).get("h6", 0) or 0),
                "volume_h1": float(pair.get("volume", {}).get("h1", 0) or 0),
                "txns_buys_h24": int(pair.get("txns", {}).get("h24", {}).get("buys", 0) or 0),
                "txns_sells_h24": int(pair.get("txns", {}).get("h24", {}).get("sells", 0) or 0),
                "price_usd": float(pair.get("priceUsd", 0) or 0),
                "price_native": float(pair.get("priceNative", 0) or 0),
                "fdv": float(pair.get("fdv", 0) or 0),
                "market_cap": float(pair.get("marketCap", 0) or 0),
                "price_change_h24": float(pair.get("priceChange", {}).get("h24", 0) or 0),
                "price_change_h6": float(pair.get("priceChange", {}).get("h6", 0) or 0),
                "price_change_h1": float(pair.get("priceChange", {}).get("h1", 0) or 0),
                "price_change_m5": float(pair.get("priceChange", {}).get("m5", 0) or 0),
                "synced_at": signal,
            })

        # 限速控制：最多 55 req/min（留余量）
        if i > 0 and i % 55 == 0:
            time.sleep(60)

    # 3. 批量写入
    if rows:
        for i in range(0, len(rows), 100):
            batch = rows[i:i+100]
            df = pd.DataFrame(batch)
            self.clickhouse.save_dataframe("token_liquidity", df)
        logger.info(f"token_liquidity: saved {len(rows)} rows")
    else:
        logger.warning("token_liquidity: no data")

    res_dict[tid] = True
```

### 4.3 限速控制

| 场景 | 处理 |
|------|------|
| 正常同步 | 每 55 次请求暂停 60 秒 |
| 单请求失败 | `try/except` 跳过，不影响其他代币 |
| 全部 665 个代币 | 约 11 分钟完成（含等待） |
| 新的永续合约 | 下次同步自动覆盖 |

## 五、查询示例

```sql
-- 查询某个代币的所有 DEX 流动性池（按流动性降序）
SELECT chain, dex_id, liquidity_usd, volume_h24
FROM token_liquidity FINAL
WHERE symbol = 'ZRO'
ORDER BY liquidity_usd DESC;

-- 查询即将解锁且 DEX 流动性不足的代币
SELECT u.symbol, u.next_unlock_time, u.next_unlock_amount_usd,
       SUM(l.liquidity_usd) AS total_dex_liquidity
FROM token_unlock FINAL u
LEFT JOIN token_liquidity FINAL l ON u.symbol = l.symbol
WHERE u.next_unlock_time >= now()
  AND u.next_unlock_time <= now() + INTERVAL 14 DAY
  AND u.binance_code != ''
GROUP BY u.symbol, u.next_unlock_time, u.next_unlock_amount_usd
HAVING total_dex_liquidity < u.next_unlock_amount_usd * 10
ORDER BY u.next_unlock_amount_usd DESC;

-- 查询某条链上的流动性分布
SELECT chain, COUNT(*) AS pool_count,
       SUM(liquidity_usd) AS total_liquidity
FROM token_liquidity FINAL
WHERE symbol = 'PEPE'
GROUP BY chain
ORDER BY total_liquidity DESC;
```

## 六、与现有系统的集成

### 6.1 修改文件清单

| 文件 | 改动 |
|------|------|
| `pond/clickhouse/kline.py` | **新增** `TokenLiquidity` 模型类 |
| `pond/clickhouse/helper.py` | **新增** `__sync_token_liquidity()` + `sync_token_liquidity()`；**新增** `from pond.token_unlock import UnlockFilter` 复用 baseAsset 提取 |

### 6.2 与 TokenUnlock 的关系

| 组件 | 数据 | 用途 |
|------|------|------|
| `token_unlock` 表 | 解锁事件时间表 | 何时解锁、解锁多少 |
| `token_liquidity` 表 | DEX 流动性快照 | 解锁时的流动性深度 |
| `get_token_unlock_exclusions()` | 大额解锁 × 低流动性 | 综合风控排除列表 |

联合查询可以回答：**"即将解锁的量是否超过该代币 DEX 流动性的合理比例？"**

## 七、实现计划

### P0（核心流程）

- [ ] `kline.py` 定义 `TokenLiquidity`（继承 `Base`，ReplacingMergeTree，按月分区）
- [ ] `helper.py` 实现 `__sync_token_liquidity()` — DEX Screener search → 映射 → `save_dataframe()`
- [ ] `sync()` 入口注册 `"token_liquidity"` 分支

### P1（优化 + 集成）

- [ ] 限速控制：按 55 req/min 分批 + 间隔暂停
- [ ] 策略层封装 `get_liquidity_risk()` — 结合 unlock 数据的流动性风险评估
- [ ] 定时同步（cron / scheduler）
