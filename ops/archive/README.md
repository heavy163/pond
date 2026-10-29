# ClickHouse → SQLite 电子存档

> 目的：本机同时跑 ClickHouse、交易库、MQTT Broker。**整机宕机 = 数据不可逆中断**。
> 本目录把不可再生的数据按天导成 SQLite 单文件，可整包上传云服务器存 6 个月；
> 需要时从 SQLite 原样灌回 ClickHouse。

---

## 1. 为什么用 SQLite

| 方案 | 问题 |
|---|---|
| ClickHouse BACKUP 到 `File` | 备份文件是 CH 私有格式，必须同版本 CH 才能恢复；云服务器上没 CH 就是死数据 |
| `clickhouse-client` 导 TSV + gzip | 能读，但恢复要自己写建表 + 类型转换，且不能增量查 |
| **SQLite** | 单文件、零依赖、任何语言任何机器都能读；能直接 SQL 查；`sqlite3` 是 Python 标准库 |

代价：SQLite 单文件写入是串行的，所以只适合归档（写一次、读多次），不适合当在线库。

---

## 2. 归档范围

分两种模式，区别只在「保留多久」：

| 模式 | 语义 | 表 |
|---|---|---|
| `full` | 全历史，永久保留 | `future_position`、`future_wallet`、`future_info`、`future_long_short_position_ratio`、`market_regime`、`stock_strategy_signal` |
| `rolling` | 只留最近 N 个月（默认 6） | `kline_futures_1h`、`kline_spot_1h`、`future_open_interest`、`future_funding_rate`、`future_long_short_ratio`、`futures_prediction`、`futures_prediction_v2` |

**为什么这么分**：

- `future_position` / `future_wallet` 是自己的交易记录，**全世界只有这一份**，丢了永远没有 → 全历史。
- `future_open_interest` / `future_long_short_ratio` 只有快照，交易所不提供历史回补 → 必须归档，但体积大 → 6 个月滚动。
- K 线理论上能重新拉，但拉 6 年要几天且会触发限速 → 6 个月滚动。

**没有归档的**（体积太大，且可重新生成）：

| 表 | 大小 | 原因 |
|---|---|---|
| `1h_generated_v3_V3_0_0` | 50.4 GiB | 因子表，可由原始行情重算 |
| `stock_main_board_5m` | 29.9 GiB | A 股 5m，baostock 可重拉 |
| `baostock_kline_5m` | 11.7 GiB | 同上 |
| `factor_online_v4` / `factor_oi_v1` | 6.5 GiB | 因子，可重算 |

> 这四类加起来 98 GiB，占全库 96%。如果哪天要备份它们，应该走对象存储（S3/OSS）
> 而不是 SQLite。

---

## 3. 文件布局

```
~/quant_archive/
├── quant_full.sqlite              # 全历史小表（约 60 MB）
├── quant_rolling_202605.sqlite    # 滚动表按月切（每个约 200-400 MB）
├── quant_rolling_202606.sqlite
├── ...
└── quant_rolling_202610.sqlite    # 当月（仍在追加）
```

每个月文件内部：

```
t_<表名>       数据（列名/类型与 ClickHouse 一一对应）
_manifest      每张表的 time_col / mode / 原始 DDL / 列名 / 类型
_days          台账：(表, 日期, 行数, 导出时间)
```

**为什么按月切**：云服务器只保 6 个月 → 直接删老文件即可，不用在 SQLite 里做范围删除。

---

## 4. 命令

```bash
# 看归档范围与现状
/usr/bin/python3.12 ops/archive/ch_sqlite.py inventory

# 导出（默认最近 6 个月；重跑安全：先 DELETE 当天再插）
systemd-run --user --scope -p MemoryMax=8G --collect --quiet \
    /usr/bin/python3.12 ops/archive/ch_sqlite.py export --months 6

# 只导指定表
/usr/bin/python3.12 ops/archive/ch_sqlite.py export --tables future_wallet future_position

# 校验（SQLite 行数 vs ClickHouse FINAL 行数，逐表对数）
/usr/bin/python3.12 ops/archive/ch_sqlite.py verify

# 删除超过保留期的滚动文件
/usr/bin/python3.12 ops/archive/ch_sqlite.py prune --months 6

# 回灌（默认目标 quant_test，整表覆盖，可重复执行）
/usr/bin/python3.12 ops/archive/ch_sqlite.py restore --target quant_test
```

**回灌到生产库**需要 `--force` 且显式指定目标：

```bash
/usr/bin/python3.12 ops/archive/ch_sqlite.py restore --target quant --force
```

---

## 5. 灾难恢复流程（电脑挂了）

1. 把 `~/quant_archive/*.sqlite` 从云服务器下载到任意一台机器（不需要装 ClickHouse）。
2. 新机器装 ClickHouse（任意 22.x 以上版本），启动。
3. 拉本仓库，复制归档文件到 `~/quant_archive/`。
4. 先恢复到测试库验证：
   ```bash
   /usr/bin/python3.12 ops/archive/ch_sqlite.py restore --target quant_test
   /usr/bin/python3.12 ops/archive/ch_sqlite.py verify
   ```
5. 验证通过后回灌生产：
   ```bash
   /usr/bin/python3.12 ops/archive/ch_sqlite.py restore --target quant --force
   ```

**不需要本工具的情况**：只想看数据 → 直接 `sqlite3 quant_full.sqlite 'select * from t_future_wallet limit 10'`。
SQLite 是零依赖的，笔记本、手机、任何机器都能打开。

---

## 6. 已知陷阱（都是实测踩过的）

### 6.1 ReplacingMergeTree 必须带 `FINAL` 导出

`future_wallet` 普通 `count()` = 142,179，`FINAL` = 141,626 —— **差 553 行重复**。
不带 `FINAL` 导出，归档里会混入未合并的重复行，回灌后行数多于源库口径。
工具已自动判断：DDL 含 `ReplacingMergeTree` 就加 `FINAL`。

### 6.2 时间一律存 epoch，不存字符串

ClickHouse 服务端时区是 `Asia/Shanghai`，SQL 里写 `'2026-10-29'` 会被解析成上海午夜而不是 UTC 午夜。
所以：

- `DateTime` → epoch 秒（INTEGER）
- `DateTime64` → epoch 毫秒（INTEGER）
- `Date` → `'YYYY-MM-DD'` 文本
- 导出窗口用 `toUnixTimestamp(col) >= lo AND < hi`，边界由服务端时区算出

### 6.3 元数据必须用 JSON 存

`Enum8('a'=1, 'b'=2)` 和 `Decimal(18, 2)` 的类型串里**含逗号**，
用 `",".join()` 存再 `split(",")` 读会错位 —— 实测表现为「Date 列收到 str」。
`_manifest.columns` / `ch_types` 现在存 JSON（旧文件仍兼容逗号格式）。

### 6.4 回灌必须整表覆盖

第一版没有 TRUNCATE，重跑一次行数就翻倍（142,179 → 283,252）。
现在默认 `TRUNCATE TABLE IF EXISTS` 再插，可重复执行；要追加用 `--append`。

### 6.5 复杂类型不支持

`Array` / `Tuple` / `Map` / `Nested` 会直接报错退出。
当前 13 张目标表都是标量列，暂时不需要；以后要归档含 `Array` 的表，
需先把类型映射补上（存 JSON 文本，回灌时用 `JSONExtract` 还原）。

---

## 7. 定时（未安装，等你发话）

当前**没有装 cron**。建议：

```cron
# 每小时增量（只导当天，很轻）
5 * * * * cd /path/to/pond && /usr/bin/python3.12 ops/archive/ch_sqlite.py export --months 6 >> ~/quant_archive/export.log 2>&1
# 每天 03:30 校验 + 清理超期文件
30 3 * * * cd /path/to/pond && /usr/bin/python3.12 ops/archive/ch_sqlite.py verify && /usr/bin/python3.12 ops/archive/ch_sqlite.py prune --months 6
```

上传云服务器需要你提供目标（对象存储还是 rsync 到某台机器），当前**没有实现上传**，
只保证文件落在 `~/quant_archive/`。

---

## 8. 还没覆盖的部分

| 对象 | 状态 | 说明 |
|---|---|---|
| **PostgreSQL 交易库**（192.168.1.200:5432） | 未接入 | 需要库名与只读账号；接入后可用同一套 SQLite 方案（`pg_to_sqlite.py`） |
| **MQTT Broker 的 retained 消息** | 未接入 | 数据落库在 PG/CH，正常路径已覆盖；broker 自身的 retained 状态（配置、最后一条消息）需单独导出 |
| 全库 98 GiB 的大表 | 故意不做 | 走对象存储更合适，见第 2 节 |

---

## 9. 与其它备份的关系

本工具**不替代** ClickHouse 原生备份。CH 的 `BACKUP TABLE ... TO Disk(...)` 能保证
part 级一致性、恢复更快，适合同机房恢复；SQLite 归档解决的是
「**换一台机器、没有 ClickHouse 也能读到数据**」这个场景。两者互补。
