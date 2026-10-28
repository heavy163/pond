# cryptos 数据层 — 组件设计（嵌入 pond 架构）

> 状态：设计稿 2026-10-27。DDL 已落地（`report/sql/cryptos_ddl.sql`，`cryptos` 库 7 张表已建）。
> **硬边界：`quant` / `quant_v4` 是生产库，本设计全程只读、零改动。** 已实测建库前后 `quant` 54 张表 / 102.001 GiB 逐字节未变。

---

## 0. 一句话答案

在 pond 里**平行新增**一条 `cryptos` 数据线（独立的 MetaData + 独立的 Manager + 9 个交易所适配器），
复用 pond 的 ORM 约定和表命名习惯，但**不修改** `FuturesHelper`（它现在服务 `quant`，继续保留）。

---

## 1. 为什么不能直接在 pond 里"加一个数据库参数"

我读了 pond 现有实现，有三个卡点，决定了必须平行新增而不是改造：

### 卡点 1：全局 metadata + `create_all` 会污染目标库

```python
# pond/clickhouse/manager.py L44-55
class ClickHouseManager:
    def __init__(self, db_uri, ...):
        self.engine = create_engine(db_uri)
        metadata.create_all(self.engine)      # ← 把【全局 metadata 里所有模型】建到该库
```

`metadata` 是 `pond/clickhouse/__init__.py` 里的全局对象，注册了 quant 全部 54 张表的模型。
如果直接 `ClickHouseManager("...:8123/cryptos")`，**会在 `cryptos` 里把 quant 的 54 张表也建一遍**。

### 卡点 2：`FuturesHelper` 硬编码 Binance 且写死 quant URI

```python
# pond/clickhouse/helper.py L1893
conn_str = f"clickhouse://default:{password}@{host}:8123/quant"
```

`FuturesHelper`（1925 行）把 Binance 的 exchange_info / kline / funding / OI / 多空比 / WS / vision 归档
全部内联在一个类里；`UMFutures`、`binance.Client`、`BinanceWSClientWrapper` 都是直接 import 的。
改造它 = 动生产管道，风险不可接受。

### 卡点 3：`TsTable` 用"列注释"做字段映射，必须遵守

```python
# pond/clickhouse/__init__.py
class TsTable(Base):
    datetime = Column(types.DateTime64, comment="时间", primary_key=True)

    def format_dataframe(self, df):
        for col in columns:
            if col.comment in df.columns and col.name not in df.columns:
                columns[col.comment] = col.name          # df 用 API 原始字段名
                df[col.comment] = self.format_col(col, df[col.comment])
        return df[all_cols]
```

也就是说：**DB 列名 = `col.name`，DataFrame 列名 = `col.comment`（通常是交易所原始字段名）**。
例如 `FutureFundingRate.datetime` 的 comment 是 `fundingTime`，`code` 的 comment 是 `symbol`。
新表沿用这个约定，才能直接复用 `format_dataframe` / `save_to_db`。

---

## 2. 分层结构

```
┌──────────────────────────────────────────────────────────────────┐
│ 调度层  systemd timer / 容器 cron                                 │
│   crypto-sync-1h.timer  (HH:02)   crypto-audit.timer (周日 03:00) │
└───────────────────────────┬──────────────────────────────────────┘
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ 编排层  pond/cryptos/sync.py :: CryptoHelper                      │
│   · 取订阅宇宙（instrument 表，每日刷新）                          │
│   · 计算回溯窗口 [now-6h, now-1h]，幂等                            │
│   · 每所一个令牌桶 + Semaphore(5)                                 │
│   · 失败重试 → 写 coverage → 退出码反映失败                        │
└───────────────────────────┬──────────────────────────────────────┘
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ 适配层  pond/cryptos/venues/*.py :: VenueAdapter                  │
│   Binance / OKX / Bybit / Gate / Bitget / HTX / Hyperliquid /    │
│   KuCoin / MEXC                                                   │
│   每个适配器只做三件事：发请求、规范化成统一 DataFrame、声明限频   │
└───────────────────────────┬──────────────────────────────────────┘
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ 存储层  pond/cryptos/manager.py :: CryptoManager                  │
│   独立 MetaData/Base → 只会建 cryptos 的 7 张表                   │
│   写入走 native client 批量 INSERT（沿用 pond.save_dataframe）     │
└───────────────────────────┬──────────────────────────────────────┘
                            ▼
                    ClickHouse `cryptos`
              （PARTITION BY (venue, 月)，可单所独立删除）
```

---

## 3. 组件清单（文件级）

```
pond/
├── cryptos/                          # ← 新增，全部是新文件，不动任何现有模块
│   ├── __init__.py                   # 导出 CryptoHelper / CryptoManager
│   ├── meta.py                       # 独立 MetaData + CryptoBase（隔离 quant 的关键）
│   ├── models.py                     # 7 张表的 clickhouse_sqlalchemy 模型
│   ├── manager.py                    # CryptoManager(ClickHouseManager 子类) + URI 守卫
│   ├── sync.py                       # CryptoHelper：窗口回溯 / 并发 / 幂等 / 覆盖率
│   ├── audit.py                      # 覆盖度体检 + 缺口回补 + 官方归档对账
│   ├── read.py                       # 研究侧：跨所对齐读取
│   ├── config.py                     # 交易所启用开关、并发/限频/回溯窗口
│   └── venues/
│       ├── base.py                   # VenueAdapter 协议 + RateLimiter + 重试装饰器
│       ├── registry.py               # name → class 映射，按 config 启用
│       ├── binance.py                # REST fapi；历史可接 binance_history/vision
│       ├── okx.py  bybit.py  gate.py
│       ├── bitget.py  htx.py  hyperliquid.py
│       └── kucoin.py  mexc.py
└── clickhouse/                       # ← 完全不改
    ├── __init__.py                   #   全局 metadata / TsTable / Base
    ├── manager.py                    #   现有 ClickHouseManager
    └── helper.py                     #   现有 FuturesHelper（继续服务 quant）

scripts/
└── sync_cryptos.py                   # CLI：--venue --interval --dry-run/--execute
```

**为什么 `meta.py` 是隔离的关键**：`metadata.create_all()` 只作用于传给它的 `MetaData` 实例。
新建一份空白 `MetaData`，quant 的 54 个模型永远不会被带到 `cryptos` 里，反之亦然。

---

## 4. 核心组件设计

### 4.1 `meta.py` — 独立元数据

```python
# pond/cryptos/meta.py
from sqlalchemy import MetaData
from clickhouse_sqlalchemy import get_declarative_base

# 全新实例，与 pond.clickhouse.metadata 完全无关
crypto_metadata = MetaData()
CryptoBase = get_declarative_base(metadata=crypto_metadata)


class CryptoTsTable(CryptoBase):
    """与 pond.TsTable 同构，但挂在独立 metadata 上。"""
    __abstract__ = True
    datetime = Column(types.DateTime64, comment="时间", primary_key=True)

    def format_dataframe(self, df): ...   # 与 TsTable 相同实现（复制而非继承，避免耦合）
```

> 选择"复制实现"而不是"继承 TsTable"：继承会把模型注册进全局 `metadata`（因为 declarative base 不同，
> 实际不会，但语义上容易混淆）。复制 20 行代码换取零耦合，值得。

### 4.2 `models.py` — 7 张表模型

字段与 `cryptos` 库 DDL 一一对应（`report/sql/cryptos_ddl.sql` 是唯一真源）。关键差异 vs quant 现有表：

| 项 | quant 现有 | **cryptos** |
|---|---|---|
| 交易所 | 无（隐含 Binance）| **`venue LowCardinality(String)`** |
| 分区 | `toYYYYMM(datetime)` | **`(venue, toYYYYMM(datetime))`** |
| 排序键 | `(datetime, code)` | **`(venue, code, open_time)`** |
| 编码 | 默认 LZ4 | **Delta/Gorilla + ZSTD(3)** |
| 幂等 | ReplacingMergeTree 无版本列 | **`ReplacingMergeTree(ingested_at)`** |

列名保持不变（`code` / `open_time` / `datetime` / `quote_volume` / `count` / `taker_buy_volume` …），
这样研究代码从 `quant.kline_futures_1h` 切到 `cryptos.kline_futures_1h` 只需改库名 + 加 `WHERE venue=`。

```python
class KlineFutures1H(CryptoTsTable):
    __tablename__ = "kline_futures_1h"

    venue     = Column(types.String, comment="venue")
    code      = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open      = Column(types.Float64, comment="open")
    ...
    datetime  = Column(types.DateTime64, comment="close_time", primary_key=True)
    ...
    ingested_at = Column(types.DateTime64, comment="ingested_at")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=(venue, func.toYYYYMM(datetime)),
            order_by=(venue, code, open_time),
        ),
    )
```

### 4.3 `venues/base.py` — 适配器协议

这是整个设计的**扩展点**：加一个交易所 = 加一个文件，不改任何编排代码。

```python
class VenueAdapter(ABC):
    name: str                      # "binance"
    rate_limit: tuple[int, int]    # (请求数, 秒) —— 每所不同

    @abstractmethod
    async def instruments(self) -> pl.DataFrame:
        """返回: [code, raw_code, base, quote, contract_type, multiplier, listed_at]"""

    @abstractmethod
    async def kline(self, symbols: list[str], start, end,
                    market: str = "perp") -> pl.DataFrame:
        """market: perp | spot
        返回列名必须是交易所原始字段（open_time/open/high/low/close/volume/...），
        由 models 的 comment 完成映射。"""

    @abstractmethod
    async def funding(self, symbols: list[str], start, end) -> pl.DataFrame:
        """返回: [symbol, fundingTime, fundingRate, markPrice]"""

    @abstractmethod
    async def open_interest(self, symbols: list[str], start, end) -> pl.DataFrame:
        """返回: [symbol, close_time, sumOpenInterest, sumOpenInterestValue]"""

    async def mark_price(self, symbols, start, end) -> pl.DataFrame:
        """可选；默认抛 NotImplementedError"""

    # 统一的辅助：把各所的分页/限频/重试封装在基类
    async def _get(self, url, params=None, json=None) -> dict: ...
```

**适配器不做任何业务判断**：不识库、不写库、不管窗口。它只回答"给定 symbol 和时间段，返回原始数据"。
好处：单测可以脱库跑（mock HTTP），换交易所不影响存储层。

各所差异集中在这里（实测得出）：

| 交易所 | 接口特征 | 必须处理的坑 |
|---|---|---|
| Binance | `fapi/v1/klines` / `fundingRate` / `openInterestHist` | OI 只留 30 天；`startTime` 超期直接 400 |
| OKX | `api/v5/market/candles` / `funding-rate-history` | 分页用 `before`（向更早翻），不是 `startTime` |
| Bybit | `v5/market/kline` / `funding/history` | OI 只留约 8 天；`limit` 200 |
| Gate | `api/v4/futures/usdt/candlesticks` | 时间戳是**秒**不是毫秒；`contract_stats` 210 天起 400 |
| Bitget | `api/v2/mix/market/candles` | `granularity` 用 `1H` 而非 `60` |
| HTX | `api.hbdm.com/linear-swap-ex` | 域名是 `hbdm.com`；funding 分页 `page_index` |
| Hyperliquid | POST `/info`，`candleSnapshot` / `fundingHistory` | funding **每小时**结算，`interval_sec=3600` |
| KuCoin | 需先 POST `bullet-public` 换 token 拉 WS（REST 不需要）| kline 的 `from/to` 必须**毫秒** |
| MEXC | `contract.mexc.com` | 与其他 REST 风格差异最大，字段名不同 |

### 4.4 `manager.py` — 带守卫的存储层

```python
class CryptoManager(ClickHouseManager):
    """只允许连 cryptos 的 ClickHouseManager。"""

    def __init__(self, db_uri, ...):
        db = urlparse(db_uri).path.lstrip("/")
        if db != "cryptos":
            raise ValueError(f"CryptoManager 只允许 cryptos 库，收到 {db}")
        super().__init__(db_uri, ...)     # 这里 create_all 只会建 crypto_metadata 的表

    def save_to_db(self, table, df): ...  # 沿用父类：native client 批量 INSERT
```

**守卫的意义**：`FuturesHelper` 的 URI 是硬编码的，一旦参数传错，后果是往生产库写脏数据。
`CryptoManager` 把"只能写 cryptos"变成构造期断言，把风险挡在进程启动那一刻。

### 4.5 `sync.py` — 编排层

```python
class CryptoHelper:
    def __init__(self, manager: CryptoManager, venues: list[str] | None = None,
                 lookback_hours: int = 6):
        self.manager = manager
        self.adapters = [make(registry[v], ...) for v in (venues or enabled_venues())]
        self.lookback = lookback_hours

    def sync_window(self, now: datetime) -> dict:
        """一次同步：所有所 × 所有合约 × 6 小时窗口。"""
        top = now.replace(minute=0, second=0, microsecond=0)
        start, end = top - timedelta(hours=self.lookback), top - timedelta(hours=1)
        # ↑ 只抓闭合 bar：end 永远落后当前整点 1 小时

        results = {}
        for ad in self.adapters:
            syms = self._universe(ad.name)          # 从 instrument 表读，不硬编码
            for name, table, fetch in (
                ("kline",     KlineFutures1H, ad.kline),
                ("spot",      KlineSpot1H,    partial(ad.kline, market="spot")),
                ("funding",   FundingRate,    ad.funding),
                ("oi",        OpenInterest,   ad.open_interest),
                ("mark",      MarkPrice,      ad.mark_price),
            ):
                df = asyncio.run(self._bounded(fetch, syms, start, end))
                if len(df):
                    df = df.with_columns(venue=ad.name,
                                         ingested_at=datetime.utcnow())
                    self.manager.save_to_db(table, df)
                    results[f"{ad.name}.{name}"] = len(df)
        self._write_coverage(start, end, results)
        return results
```

要点：
- **`_universe()` 从 `instrument` 表读**，绝不硬编码 symbol 列表（交易所每月上下架）
- **窗口固定回溯 6 小时**，进程挂 5 小时也能自愈；靠 `ReplacingMergeTree` 幂等去重
- **`end` 永远落后当前整点 1 小时**，杜绝未闭合 bar
- 每所一个 `asyncio.Semaphore(5)` + 令牌桶，互不阻塞；某所超时不拖累其他所

### 4.6 `read.py` — 研究侧

研究代码最常做的是"跨所对齐"，`read.py` 直接提供：

```python
def kline_1h(venue: str | list[str], codes: list[str] | None = None,
             start=None, end=None) -> pl.DataFrame
def funding(venue, codes=None, start=None, end=None)
def oi(venue, codes=None, start=None, end=None)
def basis(venues=("binance",), start=None, end=None) -> pl.DataFrame
    """现货与永续按 (venue, code, 小时) 对齐，返回 basis = perp_close/spot_close - 1"""
def cross_venue_momentum(base="BTC", start=None, end=None)
    """同一 base 在多所的 1h 收益率矩阵，用于跨所价差与相关性"""
```

`basis()` / `cross_venue_momentum()` 是这类数据真正的用途——**单所数据只是原料，跨所对齐才是产品**。

### 4.7 `audit.py` — 自检

| 检查 | 逻辑 | 失败动作 |
|---|---|---|
| 覆盖度 | 每 (venue, code, 日) 应有 24 根 | 写 `coverage`，缺口进回补队列 |
| 官方归档对账 | 抽 5 个合约，与 Binance `data.binance.vision` 的 1h 归档逐值比对 | 报警（`abs(a-b) <= 1e-9*max(1,|a|)`）|
| 跨所一致性 | 同 base 的 1h 收益率相关性应 >0.99 | 低于 0.9 报警（时间偏移/单位错）|
| 重复/缺失 | `count() vs uniqExact((venue,code,open_time))` | 重复率高说明幂等失效 |

---

## 5. 数据契约

### 5.1 DataFrame → DB 的列映射（沿用 pond 约定）

| DB 列 | comment（DataFrame 列名） | 来源 |
|---|---|---|
| `venue` | `venue` | 编排层填入，不由适配器产生 |
| `code` | `jj_code`（或 `symbol`）| 适配器统一成规范名 `BTC-USDT-PERP` |
| `open_time` | `open_time` | 各所 K 线起始时间，统一转 `DateTime64(3) UTC` |
| `datetime` | `close_time` / `fundingTime` | 基类必填列 |
| `ingested_at` | `ingested_at` | 编排层填当前 UTC |

### 5.2 符号规范化

统一 `code = <BASE>-<QUOTE>-<TYPE>`，例如 `BTC-USDT-PERP`、`BTC-USDT-SPOT`。
原始代码保留在 `instrument.raw_code`（`BTCUSDT` / `BTC_USDT` / `BTC-USDT-SWAP` / `XBTUSDTM`），
适配器负责双向映射。**永远不要在研究代码里出现交易所原始代码**——那是适配器的私事。

### 5.3 时间

- 全部 `DateTime64(3, 'UTC')`，不做任何时区转换
- 加密货币 7×24 无休市、无夏令时，日界统一按 UTC 00:00
- `ingested_at` 只用于去重版本，不参与研究

---

## 6. 调度与容器化（你的实盘管道目标）

### 6.1 当前（单机 systemd）

```
crypto-sync-1h.timer    每小时 HH:02  →  python -m pond.cryptos.sync --lookback 6
crypto-instruments.timer 每日 08:10   →  python -m pond.cryptos.sync --what instruments
crypto-audit.timer      每周日 03:00  →  python -m pond.cryptos.audit
```

### 6.2 容器化（未来实盘管道）

```
┌──────────────────────┐   ┌──────────────────────┐
│ crypto-collector-    │   │ crypto-collector-    │
│ binance (container)  │   │ okx (container)      │   ... 8 个
│ env: VENUE=binance   │   │ env: VENUE=okx       │
│      CRYPTOS_URI=... │   │      CRYPTOS_URI=... │
└──────────┬───────────┘   └──────────┬───────────┘
           └──────────────┬───────────┘
                          ▼
              ┌───────────────────────┐
              │ ClickHouse (共享服务) │
              │  cryptos / quant      │
              └───────────────────────┘
                          ▲
                          │
              ┌───────────┴───────────┐
              │ strategy containers   │
              │ (只读 cryptos_reader) │
              └───────────────────────┘
```

**关键设计：collector 无状态。** 同一 `VENUE` 起 2 个副本也不会写脏数据（窗口重叠 + ReplacingMergeTree 幂等），
所以扩容就是加副本，不需要分片逻辑。这是"一容器一所"真正有价值的地方——**扩展性来自无状态，不来自数据库分家**。

容器化的切换点只有一个：`CRYPTOS_URI`。把某个所指向独立实例就完成物理隔离，代码零改动：

```
binance 容器:  CRYPTOS_URI=clickhouse://...:8123/cryptos          # 共享实例
okx 容器:      CRYPTOS_URI=clickhouse://10.0.0.9:8123/cryptos     # 独立实例
```

### 6.3 权限模型（真正的资源隔离在这里）

```sql
-- 采集账号：只能写，内存受限
CREATE USER cryptos_writer IDENTIFIED BY '***'
  SETTINGS max_memory_usage = 2000000000, max_concurrent_queries_for_user = 8;
GRANT INSERT, SELECT ON cryptos.* TO cryptos_writer;

-- 策略账号：只能读，内存更宽
CREATE USER cryptos_reader IDENTIFIED BY '***'
  SETTINGS max_memory_usage = 8000000000, max_concurrent_queries_for_user = 16;
GRANT SELECT ON cryptos.* TO cryptos_reader;
```

> 注意：CH 的 quota 是"按 user"而不是"按 database"。所以**要隔离资源，必须一容器一个账号**，
> 而不是一个交易所一个 database（后者给不了任何资源隔离，见 `CH_OPS.md`）。

---

## 7. 与 quant 现有数据的关系

### 7.1 不迁移、不覆盖

`quant.kline_futures_1h`（1267 万行，Binance 加密数据）**保持原样**。新管道只写 `cryptos`。
两者可以并存一段时间：老研究读 quant，新研究读 cryptos。

### 7.2 历史回灌（可选，只读 quant）

需要把 Binance 历史搬进新结构时，用**只读**方式复制：

```sql
-- 只 SELECT quant，不写 quant
INSERT INTO cryptos.kline_futures_1h
SELECT
    'binance'                     AS venue,
    upper(replace(code, 'USDT', '')) || '-USDT-PERP' AS code,   -- 1INCHUSDT → 1INCH-USDT-PERP
    open_time, open, high, low, close, volume, datetime,
    quote_volume, count, taker_buy_volume, taker_buy_quote_volume,
    now64(3)                      AS ingested_at
FROM quant.kline_futures_1h
WHERE code LIKE '%USDT';
```

注意点：
- `code` 规范化必须有映射表兜底（`1000BONKUSDT` 这类以数字开头的，不能简单 `replace`）
- 回灌与实时写入会重叠，靠 `ReplacingMergeTree(ingested_at)` 收敛，不会双份
- **回灌前后都要校验 `quant` 的行数与字节数未变**（本设计的 DDL 脚本已内置这类守卫）

---

## 8. 实施顺序

| 阶段 | 交付 | 验收 |
|---|---|---|
| **P0** | `meta.py` + `models.py` + `manager.py` + binance 适配器 | 单所跑通 1 小时，`cryptos.kline_futures_1h` 有数据，`quant` 字节数不变 |
| **P1** | 其余 8 所适配器 + `instrument` 宇宙 | 8 所 × 全宇宙 一轮 < 10 分钟，覆盖率 100% |
| **P2** | `audit.py` + `coverage` + systemd timer | 连续 24 小时无缺口，与官方归档对账通过 |
| **P3** | `read.py`（basis / 跨所矩阵）| 能用 3 行代码画出跨所基差曲线 |
| **P4** | 容器化（每所一容器 + 独立账号）| 起 8 个容器，停掉任意 1 个不影响其他 7 个 |

---

## 9. 已落地的部分（本次）

| 项 | 状态 |
|---|---|
| `cryptos` 数据库 | ✅ 已建 |
| 7 张表 DDL（venue 分区 + 显式 codec + 版本列）| ✅ 已建（`report/sql/cryptos_ddl.sql`）|
| DDL 执行脚本（带 quant 守卫 + 前后比对）| ✅ `report/sql/apply_cryptos_ddl.py` |
| `quant` / `quant_v4` 未受影响 | ✅ 54 表 / 102.001 GiB、40 表 / 44.973 GiB 逐字节未变 |
| pond 组件代码 | ⬜ 待实现（P0 起）|

---

## 10. 设计要点回顾（给未来的自己）

1. **隔离靠三样东西**：独立库名 + 独立 CH 账号（配额）+ 独立容器。**不靠"一个交易所一个 database"**。
2. **一张表 + venue 列 + (venue, 月) 分区**：跨所查询零摩擦，单所数据可独立 `DROP PARTITION`。
3. **适配器是唯一扩展点**：加交易所 = 加一个 200 行文件，不改编排、不改存储、不改调度。
4. **绝不硬编码 symbol**：`instrument` 表每天刷新，宇宙是数据不是代码。
5. **只抓闭合 bar**：`end = 当前整点 - 1 小时`，这一条能让后面所有研究免于"数据会变"的坑。
6. **窗口回溯 6 小时**：用幂等换自愈，代价是多拉 5 倍（对 1h 数据可忽略）。

---

## 11. 历史数据怎么办

### 11.1 先把数据分成两类（这决定一切）

| 数据 | 可回溯深度（实测）| 能否补 | 补法 |
|---|---|---|---|
| **OI 持仓量** | Binance 30 天 / OKX 30 天 / Bybit ≈8 天 / Gate 180 天 | ❌ **基本补不回来** | 只能 Day 1 抓满窗口，之后靠增量积累 |
| **funding** | Binance 2019-09 / HL 2023-05 / 其余 90–180 天 | ⚠️ 部分 | 各所分页向前翻 |
| **K线 1h** | Binance 2019-09 / OKX 2018 / Bybit 2020 / Gate 2020，其余上线起 | ✅ 全量可补 | Binance 走官方归档，其余 REST 分页 |

**所以回灌的优先级不是"从最早开始"，而是"先抢不可再生的"：**

```
第 1 步  OI 60 天窗口（Binance 30 / Bybit 8 / Gate 180）—— 今天不抓，永久缺失
第 2 步  funding 全量（Binance 到 2019-09，HL 到 2023-05）
第 3 步  K线 全量（可慢，不影响任何实时决策）
```

### 11.2 回灌单位是 (venue, code)，不是时间窗

交易所 API 都是**按 symbol 组织**的（给一个 symbol 可以翻页拿它全部历史）。
按时间窗回灌会变成 N_symbol × N_window 次请求；按 symbol 回灌只需 N_symbol 次，且天然支持断点续传。

新增一张状态表（已加入 DDL）：

```sql
CREATE TABLE IF NOT EXISTS cryptos.backfill_job (
    venue       LowCardinality(String),
    data_type   LowCardinality(String),     -- kline / spot / funding / oi
    code        LowCardinality(String),
    cursor_time DateTime64(3,'UTC'),        -- 已回灌到（不含）
    target_start DateTime64(3,'UTC'),       -- 目标起点
    status      LowCardinality(String),     -- pending / running / done / failed
    rows_total  UInt64,
    err         String,
    updated_at  DateTime64(3,'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY venue
ORDER BY (venue, data_type, code);
```

回灌进程每写一批就推进 `cursor_time`，崩溃后从 cursor 继续，**永不重复从头拉**。

### 11.3 回灌与实时共存：靠 `ingested_at` 分工

同一根 bar 可能被两边都写。用版本列控制优先级：

| 来源 | `ingested_at` 取值 | 效果 |
|---|---|---|
| 实时同步 | `now64(3)` | **永远最大 → 覆盖回灌值** |
| 历史回灌 | `bar_close_time + 1s` | 只在实时没有该 bar 时生效 |

这样即使回灌在实时之后跑，也不会用旧数据覆盖刚写好的实时数据。

### 11.4 速率与工期估算

按 300 个活跃合约、5 年历史：

| 项 | 请求数 | 耗时（5 req/s 限速）|
|---|---|---|
| K线（Binance，走 vision 月包）| 300 × 60 月 = 18,000 个文件 | 约 1–2 小时（可并发，官方 CDN 无严格限频）|
| K线（其余 7 所，REST 翻页）| 7 × 300 × 44 = 92,400 次 | 约 5 小时/所串行，可并行缩到 2 小时 |
| funding（8 所）| 8 × 300 × 5 = 12,000 次 | 约 40 分钟 |
| **OI（不可再生）** | 8 × 300 × 1 = 2,400 次 | **20 分钟 —— 第一件要做的事** |
| 数据量 | 8 × 300 × 43,800 ≈ 1.05 亿行 | 压缩后约 4–6 GB |

**关键：回灌进程独立于实时进程**，用独立令牌桶（只占各所额度的 50%），保证实时 1h 同步永不被回灌挤掉。

### 11.5 Binance 走官方归档，不走 REST

`pond/binance_history/vision.py` 已经有 `data.binance.vision` 的封装。月度归档：
`https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2025-01.zip`

- 一个 zip = 一整个月，比 REST 翻页快约 100 倍
- 同时是**唯一的外部真相源**：可以用它反查我们自己录的数据对不对
- 官方归档有 checksum 文件，可校验完整性

### 11.6 回灌的正确性验收

回灌数据和实时数据在**重叠区间必须逐值一致**（都是同一根已闭合 bar）：

```sql
-- 重叠区对账：应返回 0 行
SELECT a.venue, a.code, a.open_time, a.close AS live, b.close AS backfill
FROM cryptos.kline_futures_1h a FINAL
JOIN (SELECT * FROM cryptos.kline_futures_1h_backfill FINAL) b
  ON (a.venue, a.code, a.open_time) = (b.venue, b.code, b.open_time)
WHERE abs(a.close - b.close) > 1e-9 * greatest(abs(a.close), 1)
```

> 说明：上例用 `_backfill` 影子表做对照。若不允许建影子表，则回灌前先 `SELECT` 落 parquet 留档再入库。

### 11.7 历史数据的产品化产出（回灌完才能做的东西）

| 产出 | 依赖 |
|---|---|
| 跨所基差（spot vs perp）| K线（永续 + 现货）|
| 资金费率期限结构、跨所 funding 差 | funding 全量 |
| OI 与价格的关系（挤压/去杠杆）| **OI —— 只有增量，没有长历史，要老实承认** |
| 8 所流动性对比、交易所间价差 | K线 |
| 上市初期行为（新股上市 30 天）| instrument + K线 |

**必须诚实的一条**：OI 的历史在研究里会显得"只有最近几个月"。任何用到 OI 的因子，样本外窗口都被这个限制锁死。这不是设计缺陷，是交易所不提供，无法绕过。

---

## 12. 同步完成通知（MQTT）

### 12.1 现状：dove 的消息约定

```python
# dove/dove/topic.py
topic_trade_event    = "/event/trade/"
topic_system_shutdown = "/system/shutdown/"

# dove/dove/service.py
def publish_event(self, event: Event):
    self.client.publish(f"{topic_trade_event}{self.subtopic}", jsonpickle.encode(event))

# dove/dove/client.py
def publish(self, topic, msg):
    self.mqtt_client.publish(topic=topic, payload=msg, qos=2)
```

- 传输：paho-mqtt，**QoS 2**（恰好一次）
- 序列化：`jsonpickle`（保留 Python 类型）
- 话题：`/event/trade/{subtopic}`，`subtopic` 来自配置里的 `plan`
- 事件：`DataEvent`，`name=DataEvent.DOWNLOADED`，`data` 放逗号分隔的数据类型（`"kline,open_interest"`）
- 下游：`factor_service` / `model_service_tsai` 监听 `DataEvent.DOWNLOADED` 后启动计算

**现在的粒度是"整批完成"**：一次同步结束发一条，下游不知道是哪个交易所、哪个所失败了、每个所有多少行。

### 12.2 目标：三个话题，两个粒度

| 话题 | 何时发 | 谁订阅 | 用途 |
|---|---|---|---|
| `/event/data/{plan}/{venue}` | **每个交易所写完立刻发** | 监控、单所策略、告警 | 细粒度：哪个所好了、多少行、多久、有没有缺口 |
| `/event/trade/{plan}` | 全部所完成（或部分失败）后发 | 现有 factor/model 服务 | 兼容：保持"一次同步一条"的语义，不被 8 倍放大 |
| `/state/data/{plan}/{venue}` | 每次状态变化（retained）| 新启动的消费端 | 迟到者订阅即知当前状态，不用等下一轮 |

**为什么必须保留汇总话题**：`factor_service` 收到 `DataEvent.DOWNLOADED` 就会重算因子。
如果单所事件也发到同一个话题，一小时会触发 8 次重算 —— 必须分话题，而不是靠下游自己过滤。

### 12.3 事件结构

```python
# dove/dove/event/__init__.py（新增，向后兼容）
class VenueDataEvent(DataEvent):
    SYNCED = "synced"
    PARTIAL = "partial"      # 部分成功：有行写入，但也有缺口
    FAILED = "failed"

    def __init__(self) -> None:
        super().__init__()
        self.venue = None          # "binance"
        self.window_start = None   # 同步窗口起（UTC ISO）
        self.window_end = None     # 同步窗口止（UTC ISO）
        self.status = None         # synced / partial / failed
        self.rows = {}             # {"kline": 7200, "funding": 900, "oi": 7200}
        self.codes = 0             # 本次覆盖多少个合约
        self.errors = []           # ["okx kline BTC-USDT-PERP: 429 after 3 retries"]
        self.duration_ms = 0
        self.ingested_at = None    # 落库确认时间
```

`Event.id` 已经是毫秒时间戳，**下游用它去重**（MQTT QoS 2 已保证不重，但重连重投场景仍需幂等）。

### 12.4 发送时机（这一条最容易做错）

**必须在 ClickHouse 写入确认之后发**，否则下游收到通知去查库会查不到数据。

```python
# 写库
self.manager.save_to_db(table, df)      # clickhouse_connect 同步插入，返回即已落盘
# 确认
n = self.manager.count_window(table, venue, start, end)
if n < expected * 0.99:
    status = VenueDataEvent.PARTIAL
# 再发
self.notifier.publish_venue(ev)
```

如果改走异步插入（`async_insert=1`），必须带 `wait_for_async_insert=1`，否则"发出通知时数据还在缓冲区"。

### 12.5 编排层改动（很小）

```python
# pond/cryptos/sync.py
for ad in self.adapters:              # 8 个交易所
    t0 = time.time()
    ev = VenueDataEvent()
    ev.venue, ev.window_start, ev.window_end = ad.name, start, end
    ev.service_name = self.service_name
    try:
        ev.rows = self._sync_venue(ad, start, end)     # 该所全部表
        ev.status = VenueDataEvent.SYNCED
        ev.name = VenueDataEvent.SYNCED
    except Exception as e:
        ev.status = ev.name = VenueDataEvent.FAILED
        ev.errors.append(f"{ad.name}: {e}")
    finally:
        ev.duration_ms = int((time.time() - t0) * 1000)
        ev.ingested_at = utc_now().isoformat()
        self.notifier.publish_venue(ev)      # ← 每所结束立刻发，不等其他所
        results.append(ev)

# 全部结束后，发一条汇总（保持现有下游兼容）
agg = DataEvent()
agg.service_name = self.service_name
agg.name = DataEvent.DOWNLOADED
agg.data = ",".join(sorted({k for e in results for k in e.rows}))
agg.finished_signal = utc_now().isoformat()
self.notifier.publish_event(agg)
```

**关键点**：`publish_venue` 在循环体内，不在循环外。这样 OKX 卡住 3 分钟时，Binance 的完成通知已经发出去了 —— 下游可以立刻开始处理 Binance，不用等最慢的那家。

### 12.6 失败也必须发

| 场景 | 事件 | 下游行为 |
|---|---|---|
| 某所 API 全挂 | `status=failed`, `errors=[...]` | 告警；用上一轮数据继续（1h 数据可容忍延迟）|
| 部分合约失败 | `status=partial`, `errors=[...]` | 记录缺口，下一轮 6 小时回溯自动补 |
| 全部成功 | `status=synced`, `rows={...}` | 正常触达 |

**绝不静默失败**：没有事件比"发了一条失败事件"危险得多——下游会一直等。

### 12.7 消息量

8 所 × 24 轮/天 = **192 条单所事件/天** + 24 条汇总。QoS 2 完全无压力。
retained 状态话题每所只保留最后一条，broker 内存占用为常数。

### 12.8 与现有服务的兼容

| 现有服务 | 影响 |
|---|---|
| `factor_service` | 无影响（继续监听 `/event/trade/{plan}`，一天仍只有 24 条）|
| `model_service_tsai` | 无影响 |
| `data_service` | 无影响（它是发送方，不是接收方）|
| 新增消费端 | 订阅 `/event/data/{plan}/+` 一次拿到全部所，或 `/event/data/{plan}/binance` 只看一家 |

MQTT 的 `+` 通配符让"全部所"和"单所"两种订阅都成立，不需要为每个所改代码。

---

## 13. 交易所可配置 + 独立并发控制

### 13.1 拆成三个互不干扰的问题

| 问题 | 本质 | 不能混为一谈的原因 |
|---|---|---|
| 哪些所参与同步 | **配置** | 随时可改，不需重启、不需改代码 |
| 每所并发多少 | **资源** | 由各所限额决定，与别的所无关 |
| 一轮多久跑完 | **预算** | 某所慢不能拖死整轮 |

混在一起设计必然出现"为了迁就 OKX 把 Binance 也降到 2 并发"这类错误。三者必须分开。

### 13.2 配置模型：三级优先级

```
运行时 DB 表 cryptos.venue_config   ← 最高（改一行，下一轮生效，所有容器共享）
        ↓ 覆盖
配置文件 configs/cryptos.yaml        ← 中等（默认值，进 git，可 review）
        ↓ 覆盖
代码内 dataclass 默认值              ← 最低（兜底，保证空配置也能跑）
```

**为什么把 DB 表放最高优先级**：容器化之后有 8 个采集容器，改 YAML 要重启 8 个；
写一行 `venue_config` 后，下一轮（≤1 小时）全部自动生效。这是唯一能在不碰容器的前提下动态调整的地方。

```yaml
# configs/cryptos.yaml
global:
  lookback_hours: 6
  max_total_connections: 64      # 保护本机，不是 API 限额
  retry: {max_attempts: 3, backoff_sec: [1, 2, 4], jitter: 0.3}

venues:
  binance:
    enabled: true
    concurrency: 8
    deadline_sec: 3000            # 单所 50 分钟预算
    data: [kline, spot, funding, oi, mark]
    symbols: {include: ["USDT"], exclude: [], top_n: 300}
    buckets:
      default: {mode: weight, rate: 1200, window: 60}   # 官方 2400，只用 50%
      kline:   {mode: weight, rate: 1200, window: 60}

  okx:
    enabled: true
    concurrency: 10
    data: [kline, spot, funding, oi]
    buckets:
      default: {mode: requests, rate: 10, window: 1}    # 官方 20/2s，用一半
      candles: {mode: requests, rate: 20, window: 1}

  bybit:
    enabled: false                # ← 关掉：下一轮不再拉，已写入数据保留
```

### 13.3 并发模型：每所一个独立执行单元

```
                       ┌──────────────────────────────────────────┐
 asyncio.run_once() ──▶│  for venue in enabled:  create_task(...)  │
                       └───────┬──────────┬──────────┬────────────┘
                               ▼          ▼          ▼
                      ┌──────────┐ ┌──────────┐ ┌──────────┐
                      │ binance  │ │   okx    │ │  bybit   │   ← 各自独立
                      ├──────────┤ ├──────────┤ ├──────────┤
                      │Limiter   │ │Limiter   │ │Limiter   │   ← 各自令牌桶
                      │Semaphore │ │Semaphore │ │Semaphore │   ← 各自并发数
                      │Breaker   │ │Breaker   │ │Breaker   │   ← 各自熔断
                      │Deadline  │ │Deadline  │ │Deadline  │   ← 各自时间预算
                      │Session   │ │Session   │ │Session   │   ← 各自连接池
                      └──────────┘ └──────────┘ └──────────┘
                               └──────────┬──────────┘
                                          ▼
                            gather(return_exceptions=True)
                            ← 任一所抛异常，其余所照常完成
```

**关键点**：

1. **`asyncio.gather(..., return_exceptions=True)`**：不是"所有所一起成功/失败"，而是"各所独立结算"。OKX 挂了，Binance 的通知照发、数据照写。
2. **每所一个 `aiohttp.ClientSession`**：连接池不共享，避免一所长连接占满 others 的池。
3. **全局只有一个 `Semaphore(max_total_connections=64)`**：这不是 API 限额，是保护本机 FD/内存。它必须远大于各所并发之和，否则又变成互相拖累。
4. **每所一个 `asyncio.timeout(deadline_sec)`**：超时即 cancel，标记 `status=timeout`，**不让一个所吃掉整轮**。

### 13.4 限频器：用 GCRA，不用朴素令牌桶

朴素实现（每请求 `await sleep(1/rate)`）有两个硬伤：**惊群**（N 个协程同一时刻醒来同时发）和**不公平**（后来者可能插队）。
GCRA（Generic Cell Rate Algorithm）用"理论到达时间"记账，O(1)、天然 FIFO、且能直接吃 weight：

```python
# pond/cryptos/limiter.py
class GcraLimiter:
    """rate_per_sec 每秒配额；burst 允许的突发倍数。"""
    def __init__(self, rate_per_sec: float, burst: float = 1.0):
        self.interval = 1.0 / rate_per_sec
        self.burst = burst
        self._tat = 0.0          # theoretical arrival time
        self._lock = asyncio.Lock()

    async def acquire(self, cost: float = 1.0):
        async with self._lock:
            now = time.monotonic()
            tat = max(self._tat, now)
            new_tat = tat + cost * self.interval
            allow_at = new_tat - self.burst * self.interval
            self._tat = new_tat
        delay = allow_at - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)      # 睡完再发，不会惊群
```

`cost` 就是 Binance 的 weight：`klines limit=1500` 的 weight 是 10，一次 `acquire(10)`。
这样"拉 1500 根"和"拉 100 根"自动按真实成本扣费，不会因为统一按"1 次请求"计费而超限。

**必须把 `_tat` 的推进放在锁内、睡眠放在锁外**——否则一个慢请求会持锁阻塞其他协程。

### 13.5 限频粒度是 (venue, endpoint)，不是 venue

同一个交易所内部，不同端点的限额完全不同。OKX 的 candles 是 40 req/2s，funding 只有 20 req/2s；
Binance 的 klines weight 随 `limit` 从 1 到 10 变化。所以桶必须按端点分：

```python
adapter.limiter("kline").acquire(cost=10)     # 走 kline 桶
adapter.limiter("funding").acquire(cost=1)    # 走 funding 桶，互不挤占
```

`adapter.limiter(name)` 内部是 `dict[str, GcraLimiter]`，未配置的端点落到 `default` 桶。
**一所一桶会让慢端点（OI 分页多）挤占快端点（K线）的配额**，这是实际会发生的性能损失。

### 13.6 各所限频口径（初始配置，部署前逐所复核）

| 交易所 | 口径 | 初始保守值 | 备注 |
|---|---|---|---|
| Binance | **weight/分钟**（IP）| 1200 / 60s（官方 2400 的一半）| 响应头 `X-MBX-USED-WEIGHT-1M` 可实时读 |
| OKX | requests/2s（IP，按端点）| 10 / 1s | 响应头 `X-RateLimit-Remaining` |
| Bybit | requests/5s（IP+UID）| 100 / 5s（官方 120）| 有 `X-Bapi-Limit-Status` |
| Gate | requests/window（IP）| 100 / 10s ⚠️ 待复核 | 时间戳用**秒** |
| Bitget | requests/s（IP）| 15 / 1s ⚠️ 待复核 | `granularity=1H` |
| KuCoin | requests/3s | 20 / 3s ⚠️ 待复核 | |
| HTX | requests/5s（按端点）| 50 / 5s ⚠️ 待复核 | 域名 `hbdm.com` |
| Hyperliquid | weight/分钟（IP）| 600 / 60s ⚠️ 待复核 | funding 每小时结算 |

⚠️ 标记的是我按公开文档记忆填的初值，**上线前必须逐所核对官方文档**。
但设计上不依赖这些数字准确——因为有限频自校正（下面 13.7）。

### 13.7 限频自校正（这是能不能长期跑的关键）

静态配置一定会过时：交易所改限额、我们加合约、突发流量。
所以每所必须有"看响应说话"的能力：

| 信号 | 处理 |
|---|---|
| 响应头剩余额度 < 20% | 该桶速率临时 ×0.5，持续 5 分钟 |
| `429 Too Many Requests` | 读 `Retry-After`，**该端点**暂停该时长；指数退避上限 300s |
| `418 I'm a teapot`（Binance）| **IP 被封**：该所整轮立即停止，发 `status=failed` + 告警，冷却 ≥ 10 分钟 |
| 连续 5 次 5xx | 熔断该所 300s，其余所照常 |
| `400` 参数错 | **不重试**（重试 100 次也是 400），直接记录并跳过该 symbol |

最后一条容易写错：把 400 也纳入重试，会造成"一个坏 symbol 卡住整所"，而且白白消耗配额。

### 13.8 动态启停的精确语义

| 操作 | 生效时机 | 对进行中的任务 |
|---|---|---|
| `enabled=false` | 下一轮开始 | 本轮任务**继续跑完**（避免半截数据），标记 `disabled_mid_round` |
| `enabled=true` | 下一轮开始 | 无影响 |
| `concurrency` 调大/调小 | 下一轮开始 | 本轮不变（Semaphore 已创建）|
| `symbols.top_n` 调整 | 下一轮开始 | 本轮不变 |
| 新增交易所 | 需适配器代码已存在（预置 9 个）| 下一轮创建 task |

**"下一轮生效"是刻意的**：同步过程中改配置会导致同一轮内口径不一致（部分 symbol 用旧参数、部分用新参数），
数据难以解释。宁可延迟 1 小时，也不要半途改口径。

若需要秒级生效：`kill -HUP` → 重读配置 → 立刻 cancel 当前轮并重跑（会重复拉，但幂等）。

### 13.9 时间预算

轮次周期 1 小时，但**必须在 55 分钟内跑完**，留 5 分钟给写库与通知。

```
每所 deadline_sec = 3000（50 分钟）
超时 → cancel → status=timeout → 该所本轮数据部分写入（已写的不回滚）
整轮硬上限 = 3300s，到点强制收尾发汇总事件
```

`status=timeout` 的所，下一轮 6 小时回溯窗口会自动补上——**这就是回溯窗口存在的意义，不只是防进程崩溃**。

### 13.10 观测：先回答"谁拖慢了整轮"

```sql
-- 最近 24 小时，每所每轮耗时与配额使用
SELECT venue,
       count()                         AS rounds,
       avg(duration_ms) / 1000         AS avg_sec,
       max(duration_ms) / 1000         AS max_sec,
       sum(http_429)                   AS n_429,
       sum(rows)                       AS rows
FROM cryptos.sync_stat
WHERE window_start > now() - INTERVAL 1 DAY
GROUP BY venue ORDER BY avg_sec DESC
```

没有这张表，"哪个所慢"只能靠猜；有了它，扩容/限流的决策才有依据。

### 13.11 代码骨架（文件级）

```
pond/cryptos/
├── config.py       # VenueSpec / BucketSpec / load_config()（DB > YAML > 默认）
├── limiter.py      # GcraLimiter + CircuitBreaker
├── sync.py         # run_round(): 每所一个 task，gather 隔离
└── venues/base.py  # VenueAdapter: self.limiter(name).acquire(cost) + observe(resp)
```

```python
# sync.py 核心
async def run_round(self, now):
    start, end = self._window(now)
    specs = {v: s for v, s in self.config.venues.items() if s.enabled}
    tasks = {n: asyncio.create_task(self._run_venue(s, start, end), name=n)
             for n, s in specs.items()}
    done = await asyncio.gather(*tasks.values(), return_exceptions=True)
    # 每个所自己在 _run_venue 内 publish_venue，这里只发汇总
    self._publish_aggregate(done, start, end)
```

**`create_task` 而不是 `gather(coro)`**：task 有名字，`asyncio.all_tasks()` 可观测，且能单独 cancel。
排查"哪个所卡住"时，`asyncio.all_tasks()` 一行就能看到。

---

## 14. 实现落地（P0 已完成，含与设计的偏差）

> 代码位置：`pond/cryptos/`（不在 StockTrade 的 report 里，已经整体迁移到本仓库）。

### 14.1 实际文件树

```
pond/
├── doc/crypto_multivenue_sync.md     # 本文档
├── sql/cryptos_ddl.sql               # 唯一真源
├── configs/cryptos.yaml              # 默认配置
├── scripts/
│   ├── apply_cryptos_ddl.py          # 建库建表（带 quant 守卫）
│   └── sync_cryptos.py               # 采集 CLI（默认 dry-run）
├── pond/cryptos/
│   ├── config.py                     # VenueSpec / BucketSpec / 三级优先级
│   ├── limiter.py                    # GcraLimiter + CircuitBreaker
│   ├── schema.py                     # 列清单 + 时间工具 + 行校验
│   ├── store.py                      # CryptoStore（clickhouse_connect）
│   ├── notify.py                     # MQTT（dove 优先，缺失则降级日志）
│   ├── sync.py                       # CryptoHelper（编排）
│   └── venues/
│       ├── base.py                   # VenueAdapter（限频/重试/配额自校正）
│       ├── registry.py               # name → class
│       └── binance.py                # 第一个适配器
└── tests/test_cryptos.py             # 11 个用例，全离线
```

### 14.2 四处与设计不一致的地方（都是实测后改的）

| 设计原案 | 实际实现 | 原因 |
|---|---|---|
| `CryptoManager(ClickHouseManager)` 子类 + ORM 模型 | `CryptoStore` 直连 `clickhouse_connect` | `pond/clickhouse/manager.py` 顶层 `import ray, akshare, clickhouse_driver`，采集容器全不需要；且它 `__init__` 里 `metadata.create_all()` 会把全局表建进目标库 |
| 适配器返回 API 原始列名，靠 ORM 的 `comment` 映射 | 适配器直接返回**规范列名** | 弃用 ORM 后 comment 机制不存在；显式列名更可测（`validate_rows` 缺列直接报错，不静默填 0）|
| funding 的 `datetime` 直接用 API 的 `fundingTime` | **对齐到整点** `snap_to_hour()` | 实测 Binance 返回 `1793116800002`（带 2ms 抖动），原样入库会让 `JOIN ON (code, datetime)` 必然匹配失败 |
| 一个 `CryptoStore` 一个 client | **按线程隔离 client** | `clickhouse_connect` 的 client 非线程安全，而写库走 `asyncio.to_thread`，多线程并发会报 `Attempt to execute concurrent queries within the same session`（实测踩到）|

### 14.3 实测结果（2026-10-27，真实网络 + 真实 ClickHouse）

| 项 | 实测 |
|---|---|
| instrument | **523** 个 USDT 永续 TRADING（exchangeInfo 936 个合约中筛出）|
| 3 合约 × 6h | 39 行，2.9s，0 错误 |
| **30 合约 × 6h** | **412 行（kline 180 / funding 52 / oi 180），43.4s，0 错误** |
| 523 合约外推 | ≈13 分钟，占 50 分钟预算 26% |
| 幂等 | 连跑两遍 → `OPTIMIZE FINAL` 后 `count() == uniqExact()` |
| 时间正确性 | 最后一根 bar `open_time < toStartOfHour(now())`，无未闭合 bar |
| funding 对齐 | `toUnixTimestamp64Milli(datetime) % 1000 == 0` |
| quant 影响 | `quant` 33 表 / 109.5 GB、`quant_v4` 9 表 / 48.3 GB 前后逐字节未变 |

### 14.4 运行方式

```bash
cd pond

# 建库建表（幂等，带守卫）
python scripts/apply_cryptos_ddl.py --execute

# 干跑（只拉不写）
python scripts/sync_cryptos.py --symbols BTCUSDT,ETHUSDT

# 真写（配置里 top_n=30）
python scripts/sync_cryptos.py --execute --mqtt off

# 指定交易所 + 数据类型
python scripts/sync_cryptos.py --execute --venue binance --data kline,oi

# 单测（全离线，不碰网络和库）
python -m pytest tests/test_cryptos.py -q
```

退出码：全部 `synced/partial` → 0；有 `failed/timeout` → 1，systemd 可直接据此告警。

### 14.5 已知限制（下一步要补）

| 项 | 现状 | 计划 |
|---|---|---|
| 适配器 | **8 家已接入**（OKX/Bybit/Gate/Bitget/KuCoin/MEXC/HTX/Hyperliquid），Binance 关闭 | 下一批：BitMEX/Deribit（当前网络不可达）|
| `top_n` 排序 | 按字母序（不是成交额） | P2：用上一轮 `quote_volume` 排名，改成真正的活跃合约 |
| 现货 / 标记价 | 代码已支持，配置未开 | 需要基差研究时把 `spot`/`mark` 加进 `data` |
| 回灌 | 只有表（`backfill_job`），无进程 | P2：`backfill.py`，先抢 OI 30 天窗口 |
| 审计 | 无 | P2：`audit.py` + 与 `data.binance.vision` 对账 |
| MQTT | 代码就绪，本机未装 dove → 降级为日志 | 装 dove 或在容器里挂载后自动启用 |
| 调度 | 手动 CLI | P2：systemd timer（HH:02）|

---

## 15. 八所接入结果（P0 实测，2026-10-28）

Binance 关闭（生产库 `quant.kline_futures_1h` 已有其历史），其余 8 家全部接入并写入 `cryptos`。

### 15.1 一轮全量同步（每家 30 合约 × 6 小时窗口）

| 交易所 | 状态 | 合约数 | 行数 | 耗时 | 错误 |
|---|---|---|---|---|---|
| okx | synced | 30 | 224 | 11.7s | 0 |
| bybit | synced | 30 | 378 | 15.9s | 0 |
| gate | synced | 30 | 371 | 22.4s | 0 |
| bitget | synced | 30 | 224 | 17.5s | 0 |
| kucoin | synced | 30 | 227 | 15.6s | 0 |
| mexc | synced | 30 | 218 | 20.6s | 0 |
| htx | synced | 30 | 213 | 25.9s | 0 |
| hyperliquid | synced | 30 | 390 | 31.1s | 0 |

**整轮墙钟 31 秒**（8 家并行，由最慢的 Hyperliquid 决定），远低于 50 分钟预算。
按 300 合约线性外推约 5–6 分钟。

### 15.2 OI 能力矩阵（这是各所最大的差异）

| 交易所 | OI 历史 | 实测行为 | 回补可能性 |
|---|---|---|---|
| **Gate** | **180 天** | `contract_stats` 逐小时返回 | ✅ 可回补 6 个月 |
| **Bybit** | **约 8 天** | `open-interest` 每页 200 条 | ⚠️ 只能回补一周 |
| OKX | ❌ 无 | `public/open-interest` 只给当前值 | 只能增量积累 |
| Bitget | ❌ 无 | 传入 startTime/endTime 被忽略 | 只能增量积累 |
| KuCoin | ❌ 无 | 从 `contracts/active` 取当前值 | 只能增量积累 |
| MEXC | ❌ 无 | ticker 的 `holdVol` 为当前值 | 只能增量积累 |
| HTX | ❌ 无 | `swap_open_interest` 只给当前值 | 只能增量积累 |
| Hyperliquid | ❌ 无 | `metaAndAssetCtxs` 的 ctx 为当前值 | 只能增量积累 |

对无历史接口的 6 家，适配器采用**快照模式**：每次同步写入"上一整点"的一条 OI 记录。
这也是为什么 §11 说 OI 必须 Day 1 上线——**这 6 家的 OI 历史从今天开始才有**。

### 15.3 各所接口陷阱（全部实测踩过）

| 交易所 | 陷阱 | 后果 |
|---|---|---|
| **Gate** | 时间戳是**秒**不是毫秒；`candlesticks` 不允许 `limit` 与 `from/to` 同时出现（400）| 写错会整体偏移 1000 倍 / 直接 400 |
| **MEXC** | K线是**列式**结构 `{time:[], open:[], ...}`，不是行数组 | 按行解析会得到空数据 |
| **KuCoin** | `from/to` 必须是**毫秒**；单页最多 **200** 根 | 传秒返回空数组（不报错，最难发现）|
| **Hyperliquid** | 全部 POST `/info`；`fundingHistory` 只传 `startTime`（传 `endTime` 返回空）；funding **每小时** | funding 会凭空少一半 |
| **HTX** | 域名是 `hbdm.com`；响应是 `{status, data}` 双层包装 | 取错层级拿到空 |
| **OKX** | 合约字段是 `settleCcy` 不是 `settCcy`；candles 最新在前，翻页用 `after` | 写成 settCcy 会筛出 0 个合约，且**不报错** |
| **Bitget** | `granularity=1H`（不是 60）；funding 分页用 pageNo 且最新在前 | — |
| **Bybit** | kline 最新在前；OI `intervalTime=1h` 且只留约 8 天 | — |

另外两个通用教训：
1. **空宇宙必须报警**：OKX 字段写错导致 0 个合约，最初却报 `synced` —— 现在 universe 为空会标 `partial` 并记错误。
2. **`BTC` 不能当子串匹配**：会把 `AIXBT_USDT`、`WBTC_USDT` 一起选进来。现在按 `base` 精确匹配，`raw_code` 精确次之，子串仅作兜底。

### 15.4 跨所一致性校验（最强的一条正确性证据）

同一小时（2026-10-27 23:00 UTC）8 家 BTC-USDT 收盘价：

| 交易所 | close | 相对中位价 |
|---|---|---|
| bitget | 80873.3 | 0.0 bp |
| bybit | 80873.8 | +0.1 bp |
| gate | 80868.7 | −0.5 bp |
| htx | 80872.5 | −0.0 bp |
| hyperliquid | 80889.0 | +2.0 bp |
| kucoin | 80871.2 | −0.2 bp |
| mexc | 80868.4 | −0.6 bp |
| okx | 80881.4 | +1.1 bp |

**全部分布在 ±2bp 内**。8 个独立实现解析出同一根 bar，这比任何单所自检都可靠——
时间偏移、单位（张/币）、时区任何一种错误都会远超 2bp。

时间戳抖动检查：`uniqExact(toUnixTimestamp64Milli(open_time) % 1000)` 在 kline/funding/oi 三张表均为 **0**。




---

## 16. 历史回灌（已落地）

`pond/cryptos/backfill.py` + `scripts/backfill_cryptos.py`。

```bash
# 看计划（不写库）
python scripts/backfill_cryptos.py --venue gate --data oi --days 179

# 真写
python scripts/backfill_cryptos.py --venue gate,bybit --data oi --execute

# 只补主流币
python scripts/backfill_cryptos.py --venue gate --data oi --symbols BTC,ETH,SOL --execute
```

### 16.1 实测结果

| 交易所 | 数据 | 天数 | 合约 | 行数 | 耗时 | 错误 |
|---|---|---|---|---|---|---|
| **Gate** | OI | 179 | 30（27 有数据） | **92,971** | 100s | 0 |
| **Bybit** | OI | 7 | 30 | **4,909** | 7.6s | 0 |

Gate 每个合约 **4,296 行 = 179 × 24**，逐小时无缺口。7 天 Bybit 每码 168 行。

单位换算逐值对齐（2026-10-27 12:00 UTC，AAVE-USDT）：

```
期望 coin=428,299.84  usd=56,783,992.79
库内 coin=428,299.84  usd=56,783,992.79
误差 coin=0.000000%   usd=0.000000%
```

隐含价格（USD/币）落在合理区间，说明 `open_interest(张) × quanto_multiplier` 的换算正确：

| 合约 | min | median | max |
|---|---|---|---|
| AAVE-USDT | 58.52 | 95.26 | 185.93 |
| AERO-USDT | 0.31 | 0.48 | 0.99 |
| AGLD-USDT | 0.12 | 0.17 | 0.28 |

OI 规模（USD）：AAVE 21–90M、AERO 2.5–34M、AGLD 0–1.3M —— 量级合理。

### 16.2 回灌不覆盖实时的实证

`ingested_at` 分权：回灌写 `bar时间 + 1s`，实时写 `now64(3)`。

| venue | 回灌行 | 实时行 | 合计 |
|---|---|---|---|
| gate | 92,791 | 198 | 92,989 |
| bybit | 822 | 198 | 1,020 |

重叠的 18 行（3 个合约 × 6 小时）在 `count()` 里出现两次、`uniqExact` 里只有一次——
这是 ReplacingMergeTree 尚未合并的正常现象，合并时按 `ingested_at` 取大，**实时值胜出**。

### 16.3 回灌才暴露的三个 bug

| # | Bug | 症状 | 修法 |
|---|---|---|---|
| 1 | 分页按"返回数据的最大时间戳"推进 | Gate 179 天只拿到 **50,763** 行（应 128,880），满页 1000 后直接跳到窗口末 | 改**固定时间窗**推进：`page_end = min(cursor + 1000h, end)`，不依赖返回数据 |
| 2 | Gate 的 180 天是硬边界 | `from time exceeds 180-day limit` 400，30 个合约全挂 | 上限取 **179 天**，留时间余量 |
| 3 | `quanto_multiplier` 缓存为空时默认 **1.0** | 回灌不调 `instruments()`，币数被放大 **100 倍**（AAVE 428k 币 → 42.8M 币） | 改懒加载：缺失时单查 `/contracts/{name}`，查不到**抛错**而不是兜底 |

第 3 个最危险：它不报错、不影响 USD 口径（USD 用的是官方字段），只让**币数**错 100 倍。
如果只看 USD 就发现不了。

同一个"满页跳窗"问题在 Bybit/Bitget/KuCoin 的 K 线翻页里也存在（都是按返回数据推进），
一并改成固定窗口。**这类 bug 只在回灌长窗才触发，实时 6 小时窗口永远碰不到。**

### 16.4 Bybit 的 USD 口径

Bybit 的 `open-interest` 接口只给币数，不给 USD。适配器的处理：

- **最近 1 小时**：用一次 `tickers`（全市场，1 请求）取 `lastPrice` 换算
- **历史行**：`sumOpenInterestValue = 0`

不给历史行编造 USD 值——用今天的价格乘 7 天前的 OI 会得到错的价格口径。
需要 USD 敞口时在因子层 `JOIN kline close` 重算，那里有真正的历史价格。

---

## 17. 调度与运维（已安装）

### 17.1 两个 timer

| timer | 触发 | 作用 | 实测 |
|---|---|---|---|
| `cryptos-sync.timer` | 每小时 **:02** | 8 家整轮同步（`--execute`） | 31s，8/8 synced |
| `cryptos-instruments.timer` | 每日 **00:05** | 刷新合约清单（新上架/下架）+ 补最近 2 小时 | — |

Unit 文件唯一真源在仓库 `deploy/systemd/`，安装方式是「复制到 `~/.config/systemd/user/`」，
不是手改 systemd 目录里的那份——这样换机器能一键重装。

```bash
install -m 644 deploy/systemd/*.service deploy/systemd/*.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now cryptos-sync.timer cryptos-instruments.timer
```

### 17.2 为什么是 :02

`open_time=08:00` 的 1h bar 要到 `09:00:00` 才收盘。09:02 触发时：

- `end = snap_to_hour(09:02) = 09:00` → 窗口内**恰好包含**已收盘的 08:00 bar
- 不会碰到尚未收盘的 09:00 bar（非闭合 bar 一律不写）

`:00` 触发会漏掉刚收盘的那根；`:30` 触发则多等半小时，下游因子晚半小时。

### 17.3 为什么不补跑

`Persistent=false`。机器休眠/关机错过的轮次**不补**——`lookback_hours=6` 会在下一轮
自动覆盖最近 6 小时，补跑只会让开机瞬间 8 家并发 N 轮，把交易所配额一次打满。

`oneshot` + `Unit=` 的语义也保证：上一轮没跑完时，定时器不会叠一个实例上去。

### 17.4 隔离：不让同步拖垮网关

这台机器同时跑 Hermes 网关（`hermes-gateway.service`，`OOMPolicy=stop`）。
同步是独立 unit，因此：

| 护栏 | 值 | 理由 |
|---|---|---|
| `MemoryMax` | 4G | 实测峰值 < 1G，兜底防失控 |
| `Nice` | 10 | 让出 CPU 给前台 |
| `IOSchedulingPriority` | 6 | 不抢磁盘 |
| `TimeoutStartSec` | 3600 | 配置里 `round_budget_sec=3300`，留 5 分钟收尾 |
| `RequiresMountsFor` | `/mnt/hgfs/e/workspace/FutureTradeDev` | 仓库在 HGFS 上，挂载没好就别跑 |

即使同步真的 OOM，被杀的也是它自己这个 unit，**不会连带重启网关**
（网关 OOM 会 stop，那才是灾难）。

### 17.5 运维命令

```bash
# 下一次什么时候跑
systemctl --user list-timers cryptos-sync.timer

# 最近一轮日志
journalctl --user -u cryptos-sync -n 40 --no-pager

# 滚动日志（保留最近 5000 行）
tail -50 ~/.local/state/cryptos/sync.log

# 手动补跑一轮（不影响定时器）
systemctl --user start cryptos-sync.service

# 暂停 / 恢复
systemctl --user stop cryptos-sync.timer
systemctl --user start cryptos-sync.timer

# 看失败
systemctl --user --failed
```

每轮的耗时/行数/错误落 `cryptos.sync_stat`，查历史：

```sql
SELECT venue, window_start, status, codes, rows, duration_ms, checked_at
FROM cryptos.sync_stat FINAL ORDER BY checked_at DESC LIMIT 20;
```

### 17.6 实测（首次由 systemd 拉起）

```
venue       status      codes      rows        耗时  errors
okx         synced         30       254     12.2s       0
bybit       synced         30       408     17.4s       0
gate        synced         30       401     23.4s       0
bitget      synced         30       254     17.6s       0
kucoin      synced         30       258     15.6s       0
mexc        synced         30       248     20.3s       0
htx         synced         30       243     24.6s       0
hyperliquid synced         30       390     31.0s       0
```

`ExecStart` exit=0，CPU 7.87s，墙上 31s。`sync_stat` 新增 8 行。

### 17.7 端到端验证（不是"配好了"，是"真的跑过"）

验证方式：临时加一条一次性 drop-in `OnCalendar=2026-10-28 09:10:00`，等定时器自己把
service 拉起来，然后删掉 drop-in 恢复 `:02`。

```
09:10:18  Starting cryptos-sync.service ...          ← timer 触发
09:10:50  Finished cryptos-sync.service              ← 8/8 synced，exit 0
09:10:18  LAST（list-timers 记录）                    ← 定时器确实执行过
NEXT      10:02:00                                   ← 已恢复整点 :02
```

重启后能否继续跑，取决于两个开关，都已确认：

| 开关 | 状态 | 说明 |
|---|---|---|
| `loginctl Linger` | **yes** | 用户登出/未登录时 user timer 仍运行 |
| `systemctl --user is-enabled` | enabled | 已挂进 `timers.target.wants` |

> 如果哪天 Linger 被关掉，定时器只在用户登录期间生效——这是 user timer 最常见的坑。

### 17.8 幂等重写的代价（实测）

每小时重写 6 小时窗口 = 6 倍写放大。实测重复行比例：

| venue | raw 行 | 去重后 | 比例 |
|---|---|---|---|
| okx / htx / bitget / hyperliquid | 228 | 228 | **1.00** |
| bybit / gate / mexc | 234 | 228 | 1.03 |
| kucoin | 231 | 225 | 1.03 |

Merge 跟得上，比例接近 1.0。按 300 合约外推，每小时写 8 家 × 300 × 6 ≈ 14,400 行，
对 ClickHouse 是噪声级别。**暂时不需要 `OPTIMIZE FINAL`**；等库到 GB 级再看。

真正的成本不在写入，而在**重复下载**：6 小时窗口里有 5 小时是已有的。
降到 2 小时能省 2/3 请求，但漏一轮就出现数据缺口。
考虑到 8 家馆轮只花 31 秒、配额远未用满，**保留 6 小时换容错**是划算的。

---

## 18. 数据完整性：6 小时窗口只是第一层

### 18.1 直接回答

**`lookback_hours=6` 不是完整性保障，它只是一个「盲自愈」**：

- 能补：交易所晚发布 1~2 根、进程崩了一次、手动重启
- 不能补：**停机超过 6 小时**。而且它**不留任何痕迹** —— `sync_stat` 只记「这一轮成功」，
  不记「这段时间本该有数据但没有」。**洞存在，且没人知道。**

所以完整性必须是三层，缺一层都不算保障：

| 层 | 机制 | 覆盖 | 失效场景 |
|---|---|---|---|
| L1 盲自愈 | `lookback_hours=6`，每小时重写 6 小时窗 | 短抖动（<6h） | 停机 >6h 完全失效 |
| L2 检测+修复 | 每日 00:20 `check_gaps`：按天对齐应/实，自动补 | 任意长度停机（对可补数据） | OI 那 6 家补不了 |
| L3 台账 | `cryptos.coverage` 每 (venue, code, 日) 一行 + `status` | 永久留痕 | — |

L2/L3 已实现：`pond/cryptos/gaps.py` + `scripts/check_gaps.py` + `cryptos-gapcheck.timer`。

### 18.2 停机 1~2 天到底会怎样（关键矩阵）

| 数据类型 | 停机 2 天后能否补回 | 依据 |
|---|---|---|
| **K 线** | ✅ **100% 可补** | 8 家都提供全历史 K 线，按天重拉即可 |
| **funding** | ✅ **100% 可补** | 同上，历史费率接口都有 |
| **OI（Gate）** | ✅ 可补（179 天内） | `contract_stats` 有 180 天 |
| **OI（Bybit）** | ✅ 可补（7 天内） | `open-interest` 约 8 天 |
| **OI（其余 6 家）** | ❌ **永久丢失 48 小时** | 接口只返回「当前值」，没有历史 |

这是整个架构里**唯一的硬损失**。`coverage.status='lost'` 就是给这 48 小时打的黑标：
下游做 OI 因子时必须读它，否则会把「数据缺失」当成「持仓没变」。

### 18.3 检测怎么做的（以及踩到的三个盲区）

**按天对齐**：`expected = 该合约当天应有的根数`，`actual = count()`。
不搞「首尾日一律跳过」——那样当天内部挖的洞会全部漏掉。期望值由
`[合约自身首末 bar] ∩ [当天]` 算出，首日和当日天然按实际跨度算。

三个盲区都是实测撞出来的：

| 盲区 | 症状 | 修法 |
|---|---|---|
| **自指边界** | okx OI 被删空后**完全检测不到**：边界来自表自身，表空了就没有边界 | OI/funding 用 **K线当参照时钟**：有 K线的小时才应该有 OI 快照 |
| **单合约尾部缺失** | 某合约最后 3 小时没了，但它自己的 `max(datetime)` 也退到那时，区间内部无洞 | 上界改用**全所最晚 bar**；与全所差 >24h 判「疑似下架」不算故障 |
| **发布延迟** | 每小时都把「刚收盘但还没发出来」的那根误报成缺口 | 当日期望值减 1 根容差 |

另外两个口径：

- **采集起点**：早于「该类型第一行数据」的空白不算缺口（否则上线首日会把之前几小时全报成丢失），
  这批单独标 `rampup`
- **永久 vs 可补**：由 `OI_MAX_DAYS` 决定，不靠人记

### 18.4 实测：删数据 → 检出 → 补回 → 复查

```
1) 删除 okx ETH-USDT 2026-10-27 的 3 根 1h K线
2) check_gaps --days 2          → 检出 2 个缺口
3) check_gaps --days 2 --execute → 修复补回 43 行，失败 0 个窗口
4) check_gaps --days 2          → 缺口 0 个
```

同时验证了不可补路径：okx OI 分行被删后，检测输出

```
✗ 永久  okx/oi/BTC-USDT 2026-10-27: okx 只提供当前 OI，无历史接口 → 该日 OI 永久不可恢复
```

`coverage` 台账实测（按 data_type 记账后的口径）：

| data_type | ok | gap | lost | rampup |
|---|---|---|---|---|
| kline | 528 | 0 | 0 | 0 |
| funding | 528 | 0 | 0 | 0 |
| oi | 326 | 0 | 4 | 198 |

`rampup` 198 = 6 家快照所在**上线首日**只有 1 小时 OI（不是故障）；
`lost` 4 = kucoin/mexc 有 4 个 (code, 日) 当天完全没有 OI 快照。
注意台账必须是**按类型**的：早先混成一行的写法会让 okx 的 OI 缺失把它的 K线也判成不可用。

### 18.5 每日 00:20 做什么

```bash
scripts/check_gaps.py --days 7 --execute
```

1. 扫最近 7 天（8 家 × kline/funding/oi）
2. 可补的立即补（K线/funding 全量；OI 仅 Gate/Bybit 且在窗口内）
3. 不可补的写 `coverage.status='lost'` 并在日志里点名
4. 全量重写 `coverage` 台账

耗时 <2 秒（873 行台账 + 3 类查询 × 8 家），CPU 1.9s。

### 18.6 还没解决的两件事（诚实清单）

1. **停机跨过 00:20 就会延迟一天修复**。真停机 2 天后，最长要等到次日 00:20 才补 K 线。
   若要求更短 RTO，把 gapcheck 挂到每小时（成本约 +2 秒/轮），但会让 coverage 台账膨胀。
2. **6 家快照所的 OI 没有冗余**。机器停机 = 永久丢。要消掉这个风险只有两条路：
   ① 异地/第二采集器（同一小时两边都写，`ReplacingMergeTree` 天然兼容）；
   ② 换有 OI 历史接口的所（但那就不是「同一批交易所」了）。
   **在单机部署下，这 48 小时是补不回来的，只能靠台账让下游知道。**

### 18.7 运维查询

```sql
-- 最近 7 天哪些 (venue, data_type, code, 日) 不完整
SELECT venue, data_type, code, trade_date, n_expected, n_actual, status, note
FROM cryptos.coverage FINAL
WHERE trade_date >= today() - 7 AND status NOT IN ('ok')
ORDER BY status = 'lost' DESC, trade_date DESC, venue, code
LIMIT 50;

-- 永久丢失的清单（下游必须排除这些样本）
SELECT venue, data_type, count() rows, min(trade_date) d0, max(trade_date) d1
FROM cryptos.coverage FINAL WHERE status = 'lost'
GROUP BY venue, data_type ORDER BY rows DESC;

-- 平台级体检：每家每天每类数据的完整率
SELECT * FROM cryptos.v_coverage_venue_day
WHERE trade_date >= today() - 7 AND ok_ratio < 0.98
ORDER BY trade_date DESC, venue, data_type;
```

---

## 19. 下游取数契约：完整性必须被查询，不能被推断

### 19.1 为什么不能靠「有没有这行」判断

下游最容易写错的一行代码：

```python
df = load("kline", venue, start, end)
assert len(df) == 24 * days * len(codes)   # ❌ 必然误判
```

「行不存在」至少有四种含义，下游处理完全不同：

| 情况 | 含义 | 下游该做什么 |
|---|---|---|
| 交易所那小时真没成交 | 正常（极少数） | 跳过该小时 |
| 采集漏了，但可补 | 同步故障 | **等修复**，不要当真实值 |
| 采集漏了，且不可补（6 家 OI） | 永久损失 | **样本标记不可用**，绝不能前值填充 |
| 整家停更 | 管道宕机 | 整个窗口不可用 |

`count()` 把这四者混成一个数字。**完整性只能从台账 + 参照时钟读出来。**

### 19.2 三个入口

**① SQL 视图（笔记本直接用）**

```sql
-- 每家每天每类数据的完整度
SELECT * FROM cryptos.v_coverage_venue_day
WHERE trade_date >= today() - 7 AND ok_ratio < 0.98
ORDER BY trade_date DESC;

-- 某个具体 (venue, code, 日) 到底缺什么
SELECT * FROM cryptos.v_coverage_day
WHERE venue = 'okx' AND code = 'BTC-USDT' AND trade_date >= today() - 7;
```

**② Python 门禁（因子管道用）**

```python
from pond.cryptos.quality import QualityGate, DataQualityError

gate = QualityGate(store)
q = gate.check("okx", data_type="kline", days=7, min_ratio=0.98)
if not q.usable:
    raise DataQualityError(q.reasons)     # 别在脏数据上算因子

print(q.ratio, q.missing, q.lost, q.stale_hours)
```

**③ 行级标记（决定能不能前值填充）**

```python
rows = gate.row_flags("okx", "BTC-USDT", "kline", days=7)
# [{'datetime': ..., 'present': True, 'lost': False}, ...]
fillable = [r for r in rows if not r["present"] and not r["lost"]]
```

### 19.3 `status` 语义（下游就按这个分支）

| status | 含义 | 下游动作 |
|---|---|---|
| `ok` | 完整 | 直接用 |
| `gap` | 缺，但可补 | 等下一轮修复；**不要把它当真实值** |
| `lost` | 永久缺失 | **排除该样本**；绝不能前值填充 |
| `rampup` | 采集首日 / 疑似下架 | 不是故障，按业务判断 |

`coverage` 按 **(venue, data_type, code, 日)** 记账——OI 丢了不代表 K 线丢了，
必须能分开判断。这是踩过的坑：早先混成一行时，okx 的 OI 永久缺失会把 K 线也判成不可用。

### 19.4 一个必须知道的字段陷阱

**K 线的 `datetime` 是 `close_time`（`xx:59:59.999`），整点在 `open_time`。**
OI / funding 的 `datetime` 就是整点。

跨表对齐一律用 `open_time`：

```sql
-- ❌ 永远 join 不上，且不报错
JOIN open_interest o ON o.datetime = k.datetime
-- ✅
JOIN open_interest o ON o.datetime = k.open_time
```

实测症状：OI 覆盖率恒显示 0%，但两表都有数据。`quality.py` 里用 `TS_COL` 统一了。

### 19.5 完整性的时间性（研究可复现）

同一个窗口，今天判定 `gap`，明天可能已被修复。所以：

- 台账每行带 `checked_at`
- 研究落档时必须记下取数时刻（`SELECT max(checked_at)` 或跑一次 `quality_report --json`）
- 否则「昨天的样本里少了 3 小时」这件事无法复现

### 19.6 实测（当前库）

```
kline     ok 528            # 8 家 × 33 码 × 2 天
funding   ok 528
oi        ok 326 / rampup 198 / lost 4
```

窗口级门禁（2 天）：

| venue | K线 | OI |
|---|---|---|
| bybit / gate | ✓ 100% | ✓ 100%（有回灌历史） |
| 其余 6 家 | ✓ 100% | ✗ 23~28%（OI 快照才刚上线） |

这正是下游必须看到的差别：**同一家交易所，K 线可用不代表 OI 可用。**

### 19.7 CLI

```bash
python scripts/quality_report.py --install-views        # 建视图（一次性）
python scripts/quality_report.py --days 7 --data kline  # 门禁表，退出码 0/1
python scripts/quality_report.py --venue okx --code BTC-USDT --rows --days 1
```

退出码：`0` 全部可用，`1` 有窗口不可用（CI / 因子管道可直接 `&&` 串联）。

### 19.8 一个完整例子（因子取数前）

```python
from pond.cryptos.quality import QualityGate, DataQualityError
from pond.cryptos.store import CryptoStore

store = CryptoStore.from_env()
gate = QualityGate(store)

CODES = [...]           # 你的宇宙
start, end = "2026-10-01", "2026-10-27"

for venue in ["okx", "bybit", "gate", "bitget"]:
    q = gate.check(venue, data_type="kline", days=27, min_ratio=0.99)
    if not q.usable:
        print(f"跳过 {venue}: {q.reasons}")
        continue

    # 注意用 open_time 对齐（不是 datetime）
    df = store.query(f"""
        SELECT venue, code, open_time, close, volume
        FROM cryptos.kline_futures_1h
        WHERE venue = '{venue}' AND open_time >= '{start}' AND open_time < '{end}'
    """)

    # OI 单独的可用性判断：同一家可能 K线可用、OI 不可用
    q_oi = gate.check(venue, data_type="oi", days=27, allow_lost=False)
    if not q_oi.usable:
        print(f"{venue} OI 不可用于长窗：{q_oi.reasons} → OI 因子只用短窗")
```

输出的不确定性是**可以量化的**，不是"大概能跑"：

```
okx   kline 100.00% ✓        oi  23.48% ✗（快照才上线）
bybit kline 100.00% ✓        oi 100.00% ✓（有 7 天回灌）
gate  kline 100.00% ✓        oi 100.00% ✓（有 179 天回灌）
```

### 19.9 强制力从哪来（目前没有，必须自己接）

这套契约只**提供能力**，不构成强制。当前 `cryptos` 库刚建好，**还没有任何 crypto 因子在生产上读它**——
所以"没人检查"就是现实风险：「看起来成功的脏批次」比失败的批次危险得多。

两条可选的强制路径：

| 路径 | 做法 | 强度 |
|---|---|---|
| **采集端拒绝交付** | `venue_config` 加 `min_coverage`，每轮算完写 `sync_stat.coverage_ok`；低于阈值标 `partial` 且**不发 MQTT 完成事件** | 强，下游不检查也拿不到"成功"信号 |
| 因子端前置断言 | 取数第一行 `gate.assert_usable(...)`，不通过直接 raise | 中，取决于每个下游是否老实写 |

推荐先做「采集端拒绝交付」：它把判断力放在唯一一个所有人都必须经过的地方（同步出口），
而不是分散到 N 个下游。**这条目前还没做**，是本设计已知的最后一个缺口。

---

## 20. 韩国交易所：Upbit 接入（第 9 家）

> 完整研究见 notebook：`report/manual/korea_exchange_study/korea_exchange_study.ipynb`
> （数字全部由 `scripts/collect_facts.py` 实测写入 `data/facts.json`，notebook 逐值断言）

### 20.1 结论：只有 Upbit 有"上币爆拉"

实测 24h KRW 成交额（2026-10-28）：

| 交易所 | KRW 市场 | 24h 成交额 | 份额 | 单市场深度 |
|---|---|---|---|---|
| **Upbit** | 299 | **$904M** | **62.3%** | **$2.99M** |
| Bithumb | 489 | $411M | 28.5% | $0.84M |
| Coinone | 359 | $125M | 8.7% | $0.35M |
| Gopax | 254 | $7.5M | 0.5% | $0.03M |
| digitalX（原 Korbit） | — | 接口部分 503 | — | — |

上币爆拉 = 薄订单簿 × 集中买盘。单市场深度决定订单簿多薄：Upbit 是新币开盘挂单只有
几十万美元而日成交 $3M；Gopax 全所一天 $7.5M，买盘全涌向一个币也推不动。

**Korbit ≡ digitalX**：未来资产 2026-07 收购 97.15%，08-11 改法人名，**09-16 域名与 App 全部切换**。
`api.korbit.co.kr` 现在 301 到 `lightning.digitalx.miraeasset.com`。

### 20.2 上币效应实测（NIL，2026-10-27 16:40 KST）

| 时点 | Upbit（折USD） | Bybit | 溢价 |
|---|---|---|---|
| 开盘 | $0.1187 | $0.1072 | **+10.76%** |
| 首根收盘 | — | — | +3.79% |
| 第 10 分钟 | — | — | **−0.37%** |
| 第 60 分钟 | — | — | +0.24% |
| 第 116 分钟 | $0.0993 | — | — |

首分钟成交 8.30B KRW（$6.2M），首小时 41.29B KRW（$30.8M）。
**溢价窗口 ≈ 10 分钟**，同期全球所只涨 1.8~2.3%。

**结论：这是本地流动性事件，不是重新定价；需要韩国实名 KRW 账户，外部资金无法参与。**
第三方统计显示效应本身在衰减（2026H1 首日中位 +42% → 6/15 后 +10%，
因为 Upbit 成交量 YTD −70%，交易所靠加速上币救量）。

### 20.3 接入口径

| 项 | 决策 | 理由 |
|---|---|---|
| 市场 | 只取 KRW（299 中选 28 主流） | 上币效应只在法币直连市场 |
| 数据类型 | `spot` + `fx` | 现货所无 funding/OI；额外记 KRW/USD |
| code | `BTC-KRW`（raw `KRW-BTC`） | 沿用 BASE-QUOTE 约定 |
| 汇率 | `/v1/ticker?markets=KRW-USDT` 每小时 1 条 | Upbit 无汇率历史接口，只能逐时累积 |
| 限频 | 8 req/s（官方 10） | 留 1/3 余量 |

**`fx` 是新增数据类型**：不是每个 symbol 一条，而是每个交易所一条，
所以 `_sync_type` 里特判、只调一次；`VenueAdapter.fx_rates()` 默认返回 `[]`。

新表 `cryptos.fx_rate(datetime, base, quote, rate, source, ingested_at)`。

### 20.4 落地时踩到的坑

1. **`instrument.listed_at` 非 Nullable** — Upbit 不提供上市时间，传 `None` 直接崩在
   clickhouse_connect 的 temporal writer（`'NoneType' object has no attribute 'timestamp'`）。
   已 ALTER 为 `Nullable(DateTime64)`：**未知就是 NULL，不编造时间**。
2. **`instrument` 需要 `multiplier`** — 现货补 1.0。
3. **dry-run 不校验行 schema** — dry-run 不写库，所以缺列问题只有 `--execute` 才暴露。
   这是测试缺口：应该在 `validate_rows` 阶段就校验。
4. **`top_n` 按字母序取是错的** — 8 家合约所原来是 AAVE/ADA/AERO/AGLD…
   与 Upbit 的 30 个主流币**只有 1 个交集**，Korea premium 根本算不出来。
   已把 9 家统一为同一份 30 个主流币清单；`top_n` 本身仍应按 `quote_volume` 排名（未做）。

### 20.5 Korea premium（真正有用的部分）

```
Korea premium = (Upbit KRW 价 / KRW_USD) / 全球所 USDT 价 − 1
```

实测（26 币，最新共同小时）：**中位 +12 bp、均值 +17 bp**，BTC 仅 **+7 bp**，
小币最高（DOGE +93 bp / ADA +42 bp）。大币被全球套利压平，小币反映本地散户需求。

这是现有 8 家数据做不出来的因子，但**样本量远远不够**（26 币 × 数小时）：
要 3 个月（约 5.6 万观测）才有资格算 IC。**在此之前不做任何因子结论。**

### 20.6 端到端结果

9 家一轮：全部 `synced`，Upbit 28 码 / 168 行 / **4.5 秒**（占整轮 <3%）。

```sql
SELECT venue, count(), uniqExact(code), max(open_time)
FROM cryptos.kline_spot_1h WHERE venue='upbit' GROUP BY venue;
-- upbit  168  28  2026-10-28 01:00:00
```

### 20.7 接入第 9 家时，完整性层暴露的 7 个口径 bug

接入 Upbit 本来是"加一个适配器"，结果把 §18 那三层防线里最脏的问题全炸出来了。
**这些 bug 在 8 家合约所上都不会显形**，因为它们的表结构刚好绕开了：

| # | Bug | 症状 | 修法 |
|---|---|---|---|
| 1 | 用 `datetime` 算区间 | Upbit 表 `datetime` 是收盘时刻 `xx:59:59.999`，跨度少算 1 小时 → 23:00 的洞漏检 | K线类表统一用 `open_time`（`_ts()`） |
| 2 | 时间参数按服务端时区解析 | 传 UTC datetime 被当成 Asia/Shanghai，窗口整体偏移 8 小时（`open_time >= since` 只剩 13/24 行） | 一律 `toUnixTimestamp(col) >= int(epoch)` |
| 3 | `count()` 数物理行 | ReplacingMergeTree 有重复版本 → `n_actual` 27 > `n_expected` 24 | `uniqExact(时间列)` |
| 4 | 窗口首日 `day_start` 不含 `since` | 期望 24 根、实际只统计 since 之后的 21 根 → 3 个假缺口 | `day_start = max(lo, floor, day0, since)` |
| 5 | 今日容差无条件减 1 | 「昨天 23:00 有、今天 00:00 缺」被容差吃掉，漏检 | 只在「最近一小时还没出现 + 刚过整点 15 分钟内」才减 |
| 6 | 不区分该所是否采集该类型 | Upbit 没有 funding，被要求每天交 3 条 → **56 条纯噪声 gap** | 按 `spec.data` 过滤 |
| 7 | CLI `--data` 默认值不含 `spot` | 定时任务永远不写 spot 台账（静默漏掉一整类数据） | 默认值加 `spot` |

**教训**：完整性检测的参照系一旦和数据本身同源（自己数自己），任何口径错误都会被
"两边一起错"抵消掉，看起来永远是 0 缺口。上面 7 个里有 5 个都是这个模式——
**0 缺口不等于完整，可能是检测器根本没在工作。**

验证方法只有一个：**造洞**。删掉已知的 2 个小时，看它能不能检出来、能不能补回去。

```bash
# 造洞
ALTER TABLE cryptos.kline_spot_1h DELETE
WHERE venue='upbit' AND open_time IN ('2026-10-27 23:00:00','2026-10-28 00:00:00')
# 检出（应报 28 码 × 2 小时 = 56）
python scripts/check_gaps.py --venue upbit --days 1 --data spot
# 修复 + 复查（应回到 0）
python scripts/check_gaps.py --venue upbit --days 1 --data spot --execute
```
实测：56 检出 → 补回 756 行 → 复查 0 缺口 → 台账 28 码 × 24/24 ok。

---

## 21. 未来数据与未收盘 bar：一次巡检挖出的两条污染路径

### 21.1 症状

巡检时 `sync_stat` 全部 `synced`、9 家无一报错，但库里躺着 **247 行未来数据**：

```sql
SELECT venue, count(), uniqExact(code), max(open_time)
FROM cryptos.kline_futures_1h WHERE open_time > now() GROUP BY venue;
-- mexc  247  13  2026-10-28 23:00:00     ← 当时才 04:48 UTC
```

全部是 MEXC 的 `*STOCK` 类代币（AAPLSTOCK / AALSTOCK / ABBVSTOCK …）。
直接调它的接口可以对上：**04:50 请求，它返回 04:00（未收盘）和 05:00（未来）两根**。

**危害不是"多了几行"，而是它让所有健康指标失真**：

| 指标 | 被污染后的表现 |
|---|---|
| 新鲜度 | `max(open_time)` 跑到未来 18 小时 → 看起来"数据超前"，实际掩盖了真实停更 |
| 缺口检测 | `ceiling` 取全所最晚 bar = 未来值 → 所有历史区间都"应该"有数据 → 大面积假缺口 |
| 覆盖率台账 | `n_actual` 虚高 |

这正是"同步成功 ≠ 数据正确"的典型：**`sync_stat` 只看请求有没有报错，不看写进去的是什么东西。**

### 21.2 第二条污染路径：修复在维护已经出池的码

MEXC 表里有 **56** 个码，但当前 universe 只有 **28** 个 —— 换池前按字母序选的
`4STOCK / AAPLSTOCK / 0G / 1000000BABYDOGE` 等还在表里。
`GapRepairer` 按"表里出现过的所有码"回灌，于是这些旧码每小时被重拉一次。

它和第一条互相放大：**旧码在 MEXC 上正好是返回未来 bar 的那批 → 每小时把未来数据写回来一次。**

### 21.3 四道闸（已落地）

| 层 | 函数 | 作用 |
|---|---|---|
| 抓取 | `store.clip_window(table, rows, start, end)` | 裁到 `[start, end)`，挡住**未收盘的当前小时 bar**（OHLC 还在变，比缺一根更糟） |
| 入库 | `store.drop_future_rows(table, rows)` | 唯一的 `insert()` 入口，丢 `> now + 5min`（容差只拦真未来，不杀秒级时钟偏差） |
| 修复 | `GapRepairer._repair_code` | `end = min(日末, 最近已收盘整点)` |
| 范围 | `GapRepairer.repair` | 用 `adapter.instruments()` 的**当前 universe** 过滤，出池的码不再维护 |

### 21.4 验证

```bash
# 清掉已入库的未来数据
ALTER TABLE cryptos.kline_futures_1h DELETE WHERE open_time > now();
ALTER TABLE cryptos.kline_futures_1h DELETE WHERE open_time >= toStartOfHour(now());

# 造洞 → 检出 → 修复 → 复查
ALTER TABLE cryptos.kline_futures_1h DELETE
WHERE venue='mexc' AND code='BTC-USDT'
  AND open_time IN ('2026-10-28 01:00:00','2026-10-28 02:00:00');
python scripts/check_gaps.py --venue mexc --days 1 --data kline --execute
```

实测：mexc 最新 bar 由 `04:00`（未收盘）回落到 **`03:00`（最后一个已收盘）**，
未来行 **0**、未收盘行 **0**，造洞补回 4 行。

### 21.5 对应的监控规则（Grafana 必加）

这两条应该直接做成告警，因为它们**不会被 `sync_stat` 发现**：

```sql
-- P1：出现未来数据（任何一行都说明适配器口径错了）
SELECT count() FROM cryptos.kline_futures_1h WHERE open_time > now() + INTERVAL 5 MINUTE;

-- P2：universe 码数异常增长（修复在维护出池的码，或换池没生效）
SELECT venue, uniqExact(code) FROM cryptos.kline_futures_1h
WHERE open_time >= now() - INTERVAL 1 DAY GROUP BY venue;
-- 与 cryptos.instrument 里该所当前码数对比，偏差 >10% 告警
```

---

## 22. 现货 K 线：从「只有 Upbit」到 7 家

### 22.1 起因

看板上问「bybit 没有现货数据吗」。查证：**不是交易所没有，是我们没采**。
`kline_spot_1h` 里只有 upbit——因为其余 6 家适配器的 `kline()` 第一行都是：

```python
if market != "perp":
    return []
```

Bybit `category=spot` 的 BTCUSDT K 线接口实测正常返回，只是我们从没调用过。

### 22.2 各所现货端点差异（这是工作量所在）

| 所 | 端点 | symbol 形态 | 时间单位 | 备注 |
|---|---|---|---|---|
| bybit | `/v5/market/kline?category=spot` | BTCUSDT | ms | 与永续同名，只差 category |
| okx | `/api/v5/market/candles` | BTC-USDT | ms | 永续 raw 是 `BTC-USDT-SWAP`，需去掉 `-SWAP` |
| gate | `/api/v4/spot/candlesticks` | BTC_USDT | **秒** | 返回**数组**不是 dict：`[t, quoteVol, c, h, l, o, baseVol, closed]` |
| bitget | `/api/v2/spot/market/candles` | BTCUSDT | ms | `BASE` 已含 `/api/v2`，再拼一次会 404 |
| kucoin | `api.kucoin.com/api/v1/market/candles` | BTC-USDT | 秒 | 现货是独立域名（合约在 api-futures）；返回 `[t, o, c, h, l, vol, turnover]` |
| mexc | `api.mexc.com/api/v3/klines` | BTCUSDT | ms | 现货是独立域名（合约在 contract.mexc.com） |
| htx | `api.huobi.pro/market/history/kline` | btcusdt | — | 现货是独立域名（合约在 api.hbdm.com）；**只有 size 没有 from/to** |

Hyperliquid 是纯永续 DEX，`spot` 返回 0 行，属正常。

### 22.3 实现方式

每家加一个 `_kline_spot()`，`kline(market="spot")` 分派过去；`sync` 侧无需改动——
`_fetcher()` 早就把 `market="spot"` 传下去了，缺的只是适配器实现。
配置侧只在各家 `data:` 里加 `spot`。

### 22.4 实测

| 所 | 码数 | 现货行数 | BTC 现货溢价(vs 本所永续) |
|---|---|---|---|
| bybit | 27 | 180 | +3.87 bp |
| okx | 29 | 186 | +3.35 bp |
| gate | 29 | 186 | +4.31 bp |
| bitget | 29 | 186 | +4.93 bp |
| kucoin | 29 | 186 | +3.92 bp |
| mexc | 28 | 168 | +3.87 bp |
| htx | 25 | 150 | −0.12 bp |
| upbit | 28 | 942 | 法币市场，不入对比 |

OHLC 合法性检查：`high < max(o,c)` 0 行、`low > min(o,c)` 0 行、价格 ≤0 0 行（共 2,184 行）。

### 22.5 事故：改文件把脚本改坏了，还提交了

加 `universes` 参数时用 heredoc 做文本手术，把 `check_gaps.py` 里的嵌套函数整体错位了 4 格，
文件直接 `IndentationError`。**而我提交前没有跑一次**，于是坏文件进了仓库（`9814034`），
17:02 的定时任务当场报错——同步本身正常，但缺口检测/修复整段失效。

两个教训：
1. **改完脚本必须跑一次**，哪怕只改缩进。`ast.parse()` 一行就能拦住这类错误。
2. 文本手术不要用「按行号切片」，要用「按内容定位 + 断言」。行号一错就是静默错位。

修复后 `--days 2 --execute --no-coverage`（与定时任务完全相同的参数）实测 exit 0。
