-- ============================================================
-- cryptos 数据库 DDL
--
-- 原则：
--   1. 独立库，绝不触碰 quant / quant_v4（生产）
--   2. 列名与 pond 现有 quant 表保持一致（code/open_time/datetime/...），
--      只新增 venue 列，便于研究代码平滑迁移与历史回灌
--   3. PARTITION BY (venue, 月) —— 单所数据可独立 DROP/归档/TTL
--   4. 显式 codec（Delta/Gorilla + ZSTD），不依赖默认 LZ4
--   5. ReplacingMergeTree(ingested_at) 幂等：重连/重跑/回补均安全
-- ============================================================

CREATE DATABASE IF NOT EXISTS cryptos;

-- ---------- 1. 合约元数据（驱动订阅宇宙）----------
CREATE TABLE IF NOT EXISTS cryptos.instrument
(
    venue         LowCardinality(String),
    code          LowCardinality(String),                 -- 规范名 BTC-USDT-PERP
    raw_code      String,                                 -- BTCUSDT / BTC_USDT / BTC-USDT-SWAP
    base          LowCardinality(String),
    quote         LowCardinality(String),
    contract_type LowCardinality(String),                 -- perp / spot / futures
    multiplier    Float64,
    listed_at     DateTime64(3, 'UTC'),
    delisted_at   Nullable(DateTime64(3, 'UTC')),
    ingested_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY venue
ORDER BY (venue, code, contract_type);

-- ---------- 2. 永续 1h K线 ----------
CREATE TABLE IF NOT EXISTS cryptos.kline_futures_1h
(
    venue                 LowCardinality(String),
    code                  LowCardinality(String),
    open_time             DateTime64(3, 'UTC'),
    open                  Float64 CODEC(Delta, ZSTD(3)),
    high                  Float64 CODEC(Delta, ZSTD(3)),
    low                   Float64 CODEC(Delta, ZSTD(3)),
    close                 Float64 CODEC(Delta, ZSTD(3)),
    volume                Float64 CODEC(Gorilla, ZSTD(3)),
    datetime              DateTime64(3, 'UTC'),           -- close_time，与 pond TsTable 基类一致
    quote_volume          Float64 CODEC(Gorilla, ZSTD(3)),
    count                 Float64,
    taker_buy_volume      Float64 CODEC(Gorilla, ZSTD(3)),
    taker_buy_quote_volume Float64 CODEC(Gorilla, ZSTD(3)),
    ingested_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY (venue, toYYYYMM(datetime))
ORDER BY (venue, code, open_time);

-- ---------- 3. 现货 1h K线（基差必需）----------
CREATE TABLE IF NOT EXISTS cryptos.kline_spot_1h
(
    venue                 LowCardinality(String),
    code                  LowCardinality(String),
    open_time             DateTime64(3, 'UTC'),
    open                  Float64 CODEC(Delta, ZSTD(3)),
    high                  Float64 CODEC(Delta, ZSTD(3)),
    low                   Float64 CODEC(Delta, ZSTD(3)),
    close                 Float64 CODEC(Delta, ZSTD(3)),
    volume                Float64 CODEC(Gorilla, ZSTD(3)),
    datetime              DateTime64(3, 'UTC'),
    quote_volume          Float64 CODEC(Gorilla, ZSTD(3)),
    count                 Float64,
    taker_buy_volume      Float64 CODEC(Gorilla, ZSTD(3)),
    taker_buy_quote_volume Float64 CODEC(Gorilla, ZSTD(3)),
    ingested_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY (venue, toYYYYMM(datetime))
ORDER BY (venue, code, open_time);

-- ---------- 4. 资金费率（事件流，保留原生结算周期）----------
CREATE TABLE IF NOT EXISTS cryptos.funding_rate
(
    venue         LowCardinality(String),
    code          LowCardinality(String),
    datetime      DateTime64(3, 'UTC'),                   -- fundingTime
    fundingRate   Float64 CODEC(Gorilla, ZSTD(3)),
    markPrice     Float64 CODEC(Delta, ZSTD(3)),
    interval_sec  UInt32,                                 -- 28800 / 14400 / 3600
    ingested_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY (venue, toYYYYMM(datetime))
ORDER BY (venue, code, datetime);

-- ---------- 5. 持仓量 ----------
CREATE TABLE IF NOT EXISTS cryptos.open_interest
(
    venue                LowCardinality(String),
    code                 LowCardinality(String),
    datetime             DateTime64(3, 'UTC'),
    sumOpenInterest      Float64 CODEC(Gorilla, ZSTD(3)),   -- 以币计
    sumOpenInterestValue Float64 CODEC(Gorilla, ZSTD(3)),   -- 以 USDT 计（横截面统一口径）
    ingested_at          DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY (venue, toYYYYMM(datetime))
ORDER BY (venue, code, datetime);

-- ---------- 6. 标记价 / 指数价 ----------
CREATE TABLE IF NOT EXISTS cryptos.mark_price
(
    venue        LowCardinality(String),
    code         LowCardinality(String),
    datetime     DateTime64(3, 'UTC'),
    markPrice    Nullable(Float64) CODEC(Delta, ZSTD(3)),
    indexPrice   Nullable(Float64) CODEC(Delta, ZSTD(3)),
    ingested_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY (venue, toYYYYMM(datetime))
ORDER BY (venue, code, datetime);

-- ---------- 7. 覆盖度台账（自检）----------
CREATE TABLE IF NOT EXISTS cryptos.coverage
(
    venue        LowCardinality(String),
    code         LowCardinality(String),
    data_type    LowCardinality(String),   -- kline / oi / funding
    trade_date   Date,
    n_expected   UInt16,                   -- 当天应有根数（由参照时钟算，不是常量 24）
    n_actual     UInt16,
    status       LowCardinality(String),   -- ok / gap / lost / rampup
    note         String,
    checked_at   DateTime
)
ENGINE = ReplacingMergeTree(checked_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (venue, data_type, code, trade_date);

-- ---------- 8. 回灌任务台账（断点续传）----------
-- 回灌单位是 (venue, data_type, code)，不是时间窗；每写一批推进 cursor_time
CREATE TABLE IF NOT EXISTS cryptos.backfill_job
(
    venue        LowCardinality(String),
    data_type    LowCardinality(String),   -- kline / spot / funding / oi
    code         LowCardinality(String),
    cursor_time  DateTime64(3, 'UTC'),     -- 已回灌到（不含）
    target_start DateTime64(3, 'UTC'),     -- 目标起点
    status       LowCardinality(String),   -- pending / running / done / failed
    rows_total   UInt64,
    err          String,
    updated_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY venue
ORDER BY (venue, data_type, code);

-- ---------- 9. 交易所运行时配置（覆盖 YAML，动态启停）----------
CREATE TABLE IF NOT EXISTS cryptos.venue_config
(
    venue         LowCardinality(String),
    enabled       UInt8,
    concurrency   UInt16,
    deadline_sec  UInt32,
    buckets_json  String,            -- {"default":{"mode":"weight","rate":1200,"window":60}}
    data          String,            -- "kline,spot,funding,oi"
    symbols_include String,
    symbols_exclude String,
    top_n         UInt32,
    updated_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY venue;

-- ---------- 10. 同步统计（回答"谁拖慢了整轮"）----------
CREATE TABLE IF NOT EXISTS cryptos.sync_stat
(
    venue        LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end   DateTime64(3, 'UTC'),
    status       LowCardinality(String),   -- synced/partial/failed/timeout/disabled
    requests     UInt32,
    retries      UInt32,
    http_429     UInt16,
    rows         UInt64,
    codes        UInt16,
    avg_latency_ms UInt32,
    duration_ms  UInt32,
    checked_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(checked_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (venue, window_start);

-- ---------- 权限（按交易所隔离读写，密码由部署时注入）----------
-- 采集账号：只能 INSERT
-- CREATE USER IF NOT EXISTS cryptos_writer IDENTIFIED BY '<pwd>'
--   SETTINGS max_memory_usage = 2000000000, max_concurrent_queries_for_user = 8;
-- GRANT INSERT, SELECT ON cryptos.* TO cryptos_writer;
--
-- 研究/策略账号：只能 SELECT
-- CREATE USER IF NOT EXISTS cryptos_reader IDENTIFIED BY '<pwd>'
--   SETTINGS max_memory_usage = 8000000000, max_concurrent_queries_for_user = 16;
-- GRANT SELECT ON cryptos.* TO cryptos_reader;

-- 汇率快照：目前只有 Upbit 提供 KRW/USD（1 base = rate quote）。
-- 用途：Korea premium = (Upbit KRW 价 / rate) / 全球所 USDT 价 - 1。
CREATE TABLE IF NOT EXISTS cryptos.fx_rate
(
    datetime     DateTime64(3, 'UTC'),
    base         LowCardinality(String),
    quote        LowCardinality(String),
    rate         Float64,
    source       LowCardinality(String),
    ingested_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(datetime)
ORDER BY (base, quote, datetime);
