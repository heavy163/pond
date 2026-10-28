# Grafana 数据同步监控（cryptos）

把 `cryptos` 库的采集健康度暴露到 Grafana 11.1.4（`http://192.168.1.4:3000`），
**不改动任何既有看板**，全部落在独立文件夹 `Cryptos` 下。

## 交付物

| 入口 | 地址 | 内容 |
|---|---|---|
| **全所监控** | `http://192.168.1.4:3000/d/cryptos-monitor` | 11 面板，全部全所口径（状态/健康/数据量） |
| **单所数据量** | `http://192.168.1.4:3000/d/cryptos-venue-volume` | 4 类数据各一块独立图 + 顶部下拉切换交易所 |
| 文件夹 | `Cryptos`（uid `cg2kk23urwdmob`） |
| 告警规则 | Alerting → Alert rules → folder `Cryptos`（组 `cryptos-sync`，9 条） |

数据源复用已有的 `grafana-clickhouse-datasource`（uid `edvdze371o5q8f`，
`192.168.1.200:9000` native protocol，无密码）。**没有新建数据源。**

## 重新部署

```bash
cd /mnt/hgfs/e/workspace/FutureTradeDev/pond
/usr/bin/python3.12 ops/grafana/deploy_dashboard.py   # 6 个面板（幂等，按 uid 覆盖）
/usr/bin/python3.12 ops/grafana/deploy_venue_dashboard.py  # 单所数据量看板（5 面板）
/usr/bin/python3.12 ops/grafana/deploy_alerts.py      # 9 条规则（幂等，按 title upsert）
/usr/bin/python3.12 ops/grafana/deploy_alerts.py --only 3   # 只动第 3 条
```

凭证：`~/.hermes/secrets/grafana_token`（Grafana service account token，600 权限，**不入库**）。
可用环境变量 `GRAFANA_URL` / `GRAFANA_TOKEN_FILE` 覆盖。

## 单所数据量看板（独立看板）

`http://192.168.1.4:3000/d/cryptos-venue-volume`，顶部「交易所」下拉（9 个），时间窗 3 天。

| # | 面板 | 表 | 说明 |
|---|---|---|---|
| ① | 合约 K 线 每小时根数 | `kline_futures_1h` | 应≈该所合约码数；掉到 0/砍半=采集失败 |
| ② | 现货 K 线 每小时根数 | `kline_spot_1h` | 只有 upbit 有数据 |
| ③ | 持仓量 OI 每小时条数 | `open_interest` | 快照型；断档=永久丢失 |
| ④ | 资金费率 每小时条数 | `funding_rate` | 8h 结算，多数小时为 0 属正常 |
| ⑤ | 四类 24h 汇总 | 四表 | 去重后逻辑行数 |

实测：okx → ①739 ③322 ④102 ②0；upbit → ②644 其余 0。合约所无现货、现货所无合约均属正常。

## 全所监控看板面板（11 个）

**版面：三个分区（标题栏可直接折叠）**

```
一、全所状态与健康   → ① ② ③ ④ ⑤ ⑥
二、单所视图        → ⑫ ⑬    ← 顶部「交易所」下拉切换
三、全所数据量      → ⑦ ⑧ ⑨ ⑩ ⑪
```

单所视图原先排在 y=30（第 3 屏），实际看不到，已挪到 y=16（第二屏）。

**分区一：健康度**

| # | 面板 | 判读 |
|---|---|---|
| ① | 同步状态（最近一轮） | `status != synced` 即异常 |
| ② | 数据新鲜度（滞后小时） | 健康值 1~2h（最新**已收盘**整点）；>2.5h 落后一轮，>3.5h 停更 |
| ③ | 未来/未收盘 bar | 必须是 0 |
| ④ | 覆盖率台账 | `lost` 只应出现在 `oi` |
| ⑤ | 跨所价差（BTC） | 正常 ±2bp |
| ⑥ | 各所 K 线行数（24h） | 骤降=换池或大量失败 |

**分区三：全所数据量**

| # | 面板 | 用途 |
|---|---|---|
| ⑦ | 各所每小时 K 线根数 | **最灵敏的故障发现器**：某所某小时掉到 1~2 根=那小时采集失败 |
| ⑧ | 各所 × 数据类型 24h 行数 | 一眼看出哪家缺哪类（upbit 无 oi/funding 属正常） |
| ⑨ | 各所采集码数 | 码数跳变=换池或修复在维护出池的旧码 |
| ⑩ | OI 每小时采集条数 | OI 是快照，断档即永久丢失（6 家所无历史可补） |
| ⑪ | 累计数据量（全库） | 总量参考 |



面板口径统一用 `uniqExact(...)` 而不是 `count()`：ReplacingMergeTree 在后台合并完成前会有重复版本，
`count()` 会把同一根 bar 数多次（历史上 `n_actual` 曾算出 27/24 > 100%）。

### ⑦ 在 2026-10-28 首次部署时立刻抓到的两件事

1. **10-27 10:00~17:00，okx/kucoin 每小时只有 1~2 根** —— 那是接入初期的 `--symbols BTC,ETH` 测试残留，
   不是故障，会随 24h 窗口滚出。
2. **10-27 20:00~10-28 05:00，每小时 57 个码** —— 旧字母序池（30）+ 新主流池（30，重叠 3）= 57。
   原因是当时的缺口修复任务会维护**表里出现过的所有码**，包括已经出池的旧码；06:00 修复上线后回到 29。
   这条正是 `a2f0974` 修掉的 bug，现在图上表现为一个台阶。

## 告警规则（9 条）

| 级别 | 规则 | 判据 | 为什么需要 |
|---|---|---|---|
| P1 | 整家交易所停更 | 最大滞后 > 3h | 停机 |
| P1 | 同步失败 | 最近一轮 status != synced | 请求层失败 |
| P1 | 出现未来/未收盘 bar | > 0 | **`sync_stat` 永远发现不了**，MEXC 曾写进 247 行 |
| P1 | 永久丢失新增 | `lost` 且 `data_type != 'oi'` | K线/funding 本该 100% 可补，lost=架构异常 |
| P2 | 可补缺口新增 | 近 2 日 `gap` > 0 | 等修复任务补回 |
| P2 | 跨所价差异常 | 偏离中位 > 50bp | 数据或合约错 |
| P2 | 采集码数骤降 | 近 3 个已收盘小时 vs 昨日同期 -20% | 换池未生效 |
| P2 | OI 永久丢失量激增 | 当日 oi lost > 250（基线 177） | 停机时间变长 |
| P3 | 汇率异常 | KRW/USD 日变动 > 2% | 影响 Korea premium 口径 |

**为什么 `lost` 要排除 `oi`**：6 家快照所（okx/bitget/kucoin/mexc/htx/hyperliquid）不提供
OI 历史接口，停机即永久丢。这是架构固有限制，天天响就是告警疲劳。真正的事故是
**K线/funding 出现 lost**——那三类理论上 100% 可补。

## 邮件通道（**当前未打通**）

Grafana 侧现状：
- 联系点只有一个 `email receiver`，地址仍是占位符 `<example@email.com>`
- `grafana.ini` 没有 `[smtp]` 段，`/api/frontend/settings` 里无任何 `smtp*` 字段
- 联系点测试接口返回 **HTTP 400**（无法投递）

要打通需要在 Grafana 所在机器（192.168.1.4）的 `grafana.ini` 加：

```ini
[smtp]
enabled = true
host = smtp.example.com:465
user = alert@example.com
password = <授权码，不要用登录密码>
from_address = alert@example.com
from_name = Grafana Cryptos
skip_verify = false
```

重启 Grafana 后，把 `email receiver` 的 `addresses` 改成真实收件地址（多个用分号分隔），
再跑一次联系点测试，返回 200 才算通。

飞书 webhook 后续替换时，只需新增一个 `webhook` 类型联系点 + 在
`notification policies` 里把 `Cryptos` 文件夹路由过去，规则本身不用改。
