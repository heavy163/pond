# 单机版交易所数据录制（SQLite）

**一句话**：把本机那套写 ClickHouse 的加密采集，换成写 SQLite，原样搬到云服务器上跑。
本机宕机了云上还在录；本机回来以后，把 SQLite 交给运维，只补缺失的小时，不动已有数据。

---

## 1. 为什么是「单机版录制」而不是「备份」

| | 备份（本机导出） | 本次的单机录制（云上独立采） |
|---|---|---|
| 本机宕机时 | **停止**，数据断了 | **继续采**，数据不断 |
| 依赖 | 依赖本机 ClickHouse 活着 | 只要 python3 + 网络 |
| 恢复 | 把备份灌回去 | 只补缺口，已有数据不动 |

这是关键区别：备份永远在「事后」，录制在「事前」。本机电源烧了，云上这台还在按小时往 SQLite 里写。

---

## 2. 架构

```
                     ┌────────────── 本机（会宕机）──────────────┐
   交易所 API ──────► │ pond/cryptos/venues/*  →  ClickHouse     │
        │            └──────────────────────────────────────────┘
        │
        │            ┌────────────── 云服务器（7×24）───────────┐
        └──────────► │ 同一份 venues 适配器 → SqliteStore        │
                     │   ~/recorder/cryptos.sqlite               │
                     │   每小时一轮，保留 365 天，自动清理         │
                     └──────────────────────────────────────────┘
                                     │
                          本机恢复后：restore.py 只补缺口
                                     ▼
                              ClickHouse(cryptos)
```

**复用而不是重写**：`ops/recorder/` 没有一行交易所 HTTP 代码，全部 import
`pond.cryptos.venues.*`。9 家所的限速、分页、重试、未来 bar 裁剪、现货按永续过滤
这些坑都是本机那套已经踩平的，云端行为完全一致。

唯一替换的是存储层：`SqliteStore` 用鸭子类型对齐 `CryptoStore` 的方法签名，
`CryptoHelper`（`pond/cryptos/sync.py`）原封不动地跑。

---

## 3. 文件

| 文件 | 作用 |
|---|---|
| `sqlite_store.py` | SQLite 存储后端（建表 / 写入 / 清理 / 统计） |
| `record.py` | 录制器：`once` 跑一轮、`loop` 常驻、`prune` 清理、`status` 看现状 |
| `restore.py` | 把 SQLite 恢复到本地 ClickHouse，**只补缺失行** |

---

## 4. 云服务器部署

```bash
# 1. 依赖（没有 ClickHouse、没有 MQTT）
python3 -m venv .venv
.venv/bin/pip install requests aiohttp loguru pyyaml

# 2. 拉代码
git clone <repo> && cd pond

# 3. 先跑一轮看通不通
.venv/bin/python ops/recorder/record.py once

# 4. 常驻
.venv/bin/python ops/recorder/record.py loop --retain-days 365
```

systemd 单元（推荐，比 nohup 稳）：

```ini
# /etc/systemd/system/cryptos-recorder.service
[Unit]
Description=Crypto exchange recorder (SQLite)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=recorder
WorkingDirectory=/opt/pond
ExecStart=/opt/pond/.venv/bin/python ops/recorder/record.py loop --retain-days 365
Restart=always
RestartSec=30
StandardOutput=append:/var/log/cryptos-recorder.log
StandardError=append:/var/log/cryptos-recorder.log

[Install]
WantedBy=multi-user.target
```

```bash
systemctl enable --now cryptos-recorder
journalctl -u cryptos-recorder -f
```

**时区**：SQLite 里所有时间都是 **epoch 秒（UTC）**，服务器时区不影响正确性。
按 UTC 部署即可，不需要设 `TZ`。

**磁盘**：1 年 × 9 所 × 约 800 标的 × 每小时 ≈ **每所每年 150~250 MB**，全部约 2 GB。
预留 10 GB 足够。

---

## 5. 保留期与动态清理

两个闸，都在 `prune` 里：

```bash
# 只按时间（默认一年）
record.py prune --retain-days 365

# 时间 + 体积双闸：即使还没到一年，超过 8 GB 也从最老的开始删
record.py prune --retain-days 365 --max-size-gb 8
```

`loop` 模式每 24 小时自动清理一次。

删除策略：
1. 先删 `retain_days` 之前的行
2. 若仍超过 `max_size_gb`，以 **30 天**为步长继续删最老的，最多退 24 次
3. 删超过 5,000 行就 `VACUUM` 重写文件，真正释放磁盘（不是只标记删除）

覆盖的表：`kline_futures_1h`、`kline_spot_1h`、`open_interest`、`funding_rate`、
`mark_price`、`fx_rate`、`sync_stat`。`instrument` 是元数据，不清理。

---

## 6. 恢复流程（本机宕机后）

```bash
# 1. 从云服务器拿回 SQLite
scp recorder@cloud:~/recorder/cryptos.sqlite ./

# 2. 先体检：只报缺口，不写库（默认）
python3 ops/recorder/restore.py --db ./cryptos.sqlite

#    表                       扫描行        缺失行
#    kline_futures_1h      1,234,567      48,000
#    open_interest         1,234,567      48,000
#    ...

# 3. 确认数字合理后真写
python3 ops/recorder/restore.py --db ./cryptos.sqlite --execute

# 4. 只补某段时间（比如只补宕机的那 3 天）
python3 ops/recorder/restore.py --db ./cryptos.sqlite --execute \
    --from 2026-10-26 --to 2026-10-29
```

**只补缺失**：按 (表, 交易所, 自然月) 分片，先从本地 CH 拉出该月已有的
`(code, ts)` 键，SQLite 里键不在集合中的才写入。已有的行不会重灌，既省时间也不制造多余 part。

内存只跟「一个交易所一个月」有关（约 600 码 × 744 小时 ≈ 45 万键），不会爆。

---

## 7. 实测结果

本机用 `--venues bybit` 跑了一轮真录制（2026-10-29 11:56）：

| 表 | 行数 |
|---|---|
| `kline_futures_1h` | 4,878 |
| `kline_spot_1h` | 1,728 |
| `open_interest` | 4,878 |
| `funding_rate` | 828 |
| `instrument` | 913 |
| **合计** | **13,225 行 / 1.4 MB / 396 秒** |

回灌测试（`cryptos_test`）：

```
写入 13,225 行 → 行数与源一致
人为删掉 126 行 → restore 精确识别「缺 126 行」并补回
re-run       → 缺 0 行（幂等）
```

---

## 8. 单轮可能漏 1%，靠回看自愈

实测：9 家一起跑的一轮里，bybit 有 **57 个标的 × 6 小时 = 342 行**没写进去
（占该所 1.0%），其余 7 家与本地 ClickHouse **完全一致**（`FINAL` 口径逐所相等）。

原因是 bybit 的 kline 适配器在拿不到数据时 `break` 返回空列表，不抛异常 ——
多所并发时偶发限速，这一所这一轮就静默少了几个 symbol。

**不影响最终完整性**：录制器每小时跑一轮、每轮回看 6 小时，
下一个小时会把这 6 小时重新拉一遍，缺的 symbol 自动补上（`INSERT OR REPLACE` 幂等）。
实测再跑一轮 bybit 就补齐到与本地一致。

所以要记住：**不要只跑一轮就认为数据完整**。常驻 `loop` 跑满两个小时后才稳定。
补历史缺口用 `restore.py`。

---

## 9. 注意

1. **这一版抓的是 K 线 / OI / funding / mark price**，与本地 ClickHouse 采集口径完全一致。
   本地没采的（例如逐笔成交、盘口）云端也不采——要加就得两边一起加。
2. **云端不写 MQTT**：`notifier=LogNotifier`，只写日志。避免依赖 dove broker。
3. **`venue_config` 不生效**：云上单机没有那张表，交易所启停全部来自 `configs/cryptos.yaml`。
   想动态改，就改 yaml 然后重启服务。
4. **恢复默认 dry-run**：不带 `--execute` 绝不动本地库；`--target-db` 默认 `cryptos`，
   建议先 `--target-db cryptos_test` 验证一遍。
