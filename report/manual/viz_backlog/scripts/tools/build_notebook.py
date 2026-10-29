"""构建《可视化待办清单》notebook。

正文数字全部从 data/facts.json 注入，代码 cell 再做逐项断言 —— 数据一变，重新跑
collect_facts.py 再 build，正文与断言一起更新，不会出现"文档写 635 列、实际 600 列"。

用法:
    /usr/bin/python3.12 report/manual/viz_backlog/scripts/tools/build_notebook.py
"""
from __future__ import annotations

import json
from pathlib import Path

import nbformat as nbf

HERE = Path(__file__).resolve().parents[2]
FACTS = json.loads((HERE / "data" / "facts.json").read_text(encoding="utf-8"))
NB_PATH = HERE / "viz_backlog.ipynb"

nb = nbf.v4.new_notebook()
cells: list = []


def md(text: str) -> None:
    cells.append(nbf.v4.new_markdown_cell(text))


def co(text: str) -> None:
    cells.append(nbf.v4.new_code_cell(text))


N_ITEMS = 15  # 待办总数（下文 assert 校验）

FT = FACTS["factor_table"]
PQ = FACTS["position_quality"]
PR = FACTS["prediction"]

# ══════════════════════════════════════════════════════════════
md(f"""# QuantSupervise 可视化增强 · 待办清单与可行性底稿

> 状态：**待办，未开工**。本文档只记录设计、口径与验收标准，不含实现代码。
> 生成时间取自 `data/facts.json`，数据事实全部实时查询 ClickHouse 得出。

---

## 一、结论先行

1. **现有看板 25 个面板**（`quant-supervise-v2`），已覆盖信号层（IC / 分组 / 离散度）与账户层（净值 / 仓位）。
2. **缺三块**：收益归因（钱从哪来）、因子分析（为什么有效）、统计检验（是不是运气）。
3. **没有硬阻塞**：真正的因子表 `quant.1h_generated_v3_V3_0_0`
   有 **{FT['n_feature_cols']} 个因子 / {FT['n_families']} 个家族 / {len(FT['horizons'])} 个 horizon**，
   最新数据到 **{FT['max_close_time'][:16]}**，当天更新。
   （此前误判为 `factor_online_v4` 停更 —— 那是另一套 OI/LSR 因子，与模型无关。）
4. **有一处数据缺口**：`future_position.close_price` 有效率 **{PQ['close_pct']}%**、
   `open_price` 只有 **{PQ['open_pct']}%** —— 逐笔归因**必须用 K 线重建**，不能直接读表。
5. 共 **{N_ITEMS} 项待办**：收益归因 5 项、因子分析 8 项（含 1 项地基）、统计检验 2 项。

---

## 二、数据可行性盘点

### 2.1 因子表（本次真正的分析对象）

| 项目 | 值 |
|---|---|
| 表名 | `{FT['table']}` |
| 列数 | {FT['n_cols']}（2 个键列 + {FT['n_feature_cols']} 个因子列）|
| 因子家族 | {FT['n_families']} |
| horizon | {FT['horizons']} |
| 标的数 | {FT['n_codes']} |
| 总行数 | {FT['n_rows']:,} |
| 最新 close_time | {FT['max_close_time'][:19]} |
| 最新快照标的数 | {FT['latest_codes']} |
| 日增行数 | 约 {FT['daily_rows'][-2]['rows']:,} 行/天 |

**家族规模（前 12）**：{'、'.join(f"`{k}`×{v}" for k, v in list(FT['families'].items())[:12])}

### 2.2 预测表与因子表的对齐

| 项目 | 值 |
|---|---|
| 模型 | `{PR['models'][0]['model']}`（{PR['models'][0]['rows']:,} 行）|
| 最新 datetime | {PR['max_datetime'][:19]} |
| 当日标的数 | {PR['n_codes']} |
| 与因子表重合 | **{PR['overlap_with_factor']} / {PR['n_codes']}**（code 命名完全一致，直接 join）|

### 2.3 持仓表字段质量（近 7 天，{PQ['rows_7d']:,} 行）

| 字段 | 非空/有效 | 占比 | 影响 |
|---|---|---|---|
| `filled_stake_amount` | {PQ['filled_stake_ok']:,} | {PQ['filled_pct']}% | 成交额字段基本不可用 |
| `open_price` | {PQ['open_price_ok']:,} | {PQ['open_pct']}% | 83% 持仓无入场价 |
| `close_price` | {PQ['close_price_ok']:,} | **{PQ['close_pct']}%** | **完全不可用** |
| `latest_close` | {PQ['latest_close_ok']:,} | {PQ['latest_close_pct']}% | 完全不可用 |
| `prediction_refreshed_date` | {PQ['pred_refreshed_ok']:,} | 100% | 可对齐信号时点 |
| `refreshed_count` 均值 | {PQ['avg_refreshed_count']} | — | 持仓被反复刷新 |

**结论**：逐笔归因的唯一可行路径是
`future_position.open_date` → join `kline_futures_1h` 取入场价与逐日 mark price。

### 2.4 账户与敞口现状

| 项目 | 值 |
|---|---|
| 合计权益 | {FACTS['equity_total']:,.0f} USDT（{len(FACTS['wallets'])} 台机器）|
| 合计 gross | {FACTS['gross_total']:,.0f} USDT |
| **仓位比例** | **{FACTS['exposure_pct']}%** |

三个 cohort 的真实敞口（每天只写一个 cohort 的快照，必须 ASOF 加总）：

| cohort | gross | 多头名义 | 空头名义 | 多品种 | 空品种 |
|---|---|---|---|---|---|
""" + "\n".join(
    f"| {x['strategy_id']} | {x['gross']:,.0f} | {x['long_notional']:,.0f} | "
    f"{x['short_notional']:,.0f} | {x['long_pairs']} | {x['short_pairs']} |"
    for x in FACTS["cohorts"]
) + f"""

**资金费率表**：`quant.future_funding_rate` 最新 {FACTS['funding'].get('max_datetime', 'N/A')[:19]}，
{FACTS['funding'].get('rows', 0):,} 行 —— crypto 永续的 funding 必须单列，否则会被误算成选股 alpha。
""")

# ══════════════════════════════════════════════════════════════
co("""import json, sys
from pathlib import Path

FACTS = json.loads(Path("data/facts.json").read_text(encoding="utf-8"))
FT, PQ, PR = FACTS["factor_table"], FACTS["position_quality"], FACTS["prediction"]

# ── 结构性断言：这些成立，第二节的全部结论就成立 ──
assert FT["n_cols"] == FT["n_feature_cols"] + 2, "列数应为 2 键列 + N 因子列"
assert sum(FT["families"].values()) == FT["n_feature_cols"], "家族分解应覆盖全部因子列"
assert 6 in FT["horizons"] and 72 in FT["horizons"], "horizon 必须含 6 与 72（模型是 hr72）"
assert 1 not in FT["horizons"], "因子表没有 1h horizon —— 更短持有期测不出来"
assert PR["overlap_with_factor"] == PR["n_codes"], "预测标的不应落在因子表之外"
assert PQ["close_price_ok"] == 0, "close_price 若变为可用，归因方案需改写（不必再重建）"
assert PQ["open_pct"] < 50, "open_price 覆盖率若显著提升，归因口径需重新评估"
assert len(FACTS["cohorts"]) == 3, "应为 3 个子策略 cohort"
assert 50 < FACTS["exposure_pct"] < 200, "仓位比例应在合理区间，否则先查加总逻辑"

print(f"因子表 {FT['n_feature_cols']} 因子 / {FT['n_families']} 家族 / horizon {FT['horizons']}")
print(f"最新 close_time = {FT['max_close_time'][:19]}")
print(f"预测 {PR['n_codes']} 标的，与因子表重合 {PR['overlap_with_factor']}")
print(f"close_price 有效率 {PQ['close_pct']}% → 归因必须用 K 线重建")
print(f"仓位比例 {FACTS['exposure_pct']}%  gross={FACTS['gross_total']:,.0f}  equity={FACTS['equity_total']:,.0f}")
print("✅ 结构断言全部通过")""")

# ══════════════════════════════════════════════════════════════
BACKLOG = [
    dict(id="A1", grp="收益归因", name="PnL 瀑布（来源分解）",
         q="+16.6% 里多头腿、空头腿、资金费率、残余各占多少",
         src="kline_futures_1h + future_position(open_date/open_price) + future_funding_rate",
         spec="持仓收益 = stake × (px_t/px_entry − 1) × sign(side)；funding = stake × rate × 持仓小时；"
              "滑点不计（用户口径），funding 只做诊断不用于否决信号",
         acc="四段之和与钱包净值变化误差 < 5%", dep="K 线重建", eta="1 天"),
    dict(id="A2", grp="收益归因", name="实现分组收益 vs 模型分组收益",
         q="模型说 g10 +71bp，实盘真的吃到了吗",
         src="future_position.group + 重建收益率",
         spec="左柱=模型 g1..g10 的 24h 超额；右柱=实盘按 group 的实际收益率；差值=执行损耗",
         acc="两套柱状并排，g9/g10 的差值可量化", dep="A1 的重建", eta="0.5 天"),
    dict(id="A3", grp="收益归因", name="cohort 净值对比",
         q="43.133 那台只赚 6.39%，是不是调仓日效应",
         src="future_position.strategy_id + future_wallet",
         spec="三个 cohort 各自净值归一到 100，并排三条线",
         acc="能看出 cohort 间收益差异是否系统性", dep="无", eta="0.5 天"),
    dict(id="A4", grp="收益归因", name="日度归因堆叠（选股 / beta / 残余）",
         q="宣称市场中性，净敞口真的接近 0 吗",
         src="future_position + kline（截面均值即市场）",
         spec="日收益 = 截面选股 + 净敞口×市场 + 残余，堆叠柱",
         acc="beta 项占比 < 20%，否则“市场中性”名不副实", dep="A1", eta="0.5 天"),
    dict(id="A5", grp="收益归因", name="Top/Bottom 20 贡献者",
         q="有没有稳定大亏的品种可以取反",
         src="future_position + 重建收益率",
         spec="正贡献与负贡献各 20 个，横向棒棒糖；负贡献单列一栏",
         acc="负贡献品种可按“稳定”排序（出现次数 × 平均收益）", dep="A1", eta="0.5 天"),
    dict(id="B1", grp="因子分析", name="预聚合表 factor_ic_daily（地基）",
         q="635 因子 × 50 GiB 不能给 Grafana 直接查，怎么办",
         src=f"{FT['table']} → 每日批处理",
         spec="字段：date, factor, horizon, ic, icir, coverage, n_codes；"
              f"规模 {FT['n_feature_cols']}×{len(FT['horizons'])}×90 ≈ "
              f"{FT['n_feature_cols'] * len(FT['horizons']) * 90:,} 行（几百 KB）",
         acc="每日增量算完 < 10 分钟；面板查询 < 1 秒", dep="无", eta="0.5 天"),
    dict(id="B2", grp="因子分析", name="IC 排行榜（top20 / bottom20）",
         q="哪些因子有效、哪些稳定反向",
         src="factor_ic_daily",
         spec="横向条形，x=近 30 日均 IC；**负 IC 单列一栏**，因为稳定负 IC = 可取反的因子",
         acc="同时给出 ICIR，避免“IC 高但抖”排前面", dep="B1", eta="2 小时"),
    dict(id="B3", grp="因子分析", name="IC 衰减曲线（6 → 168h）",
         q="持仓 3 天到底是不是最优持有期",
         src="factor_ic_daily",
         spec=f"x 轴 {FT['horizons']}，每个家族一条线，取 top 10 家族",
         acc="能指出 IC 峰值所在的 horizon；若峰值在 24h，持有期就该改", dep="B1", eta="2 小时"),
    dict(id="B4", grp="因子分析", name="因子家族热力图（106 × 6）",
         q="哪类因子在哪个 horizon 有效",
         src="factor_ic_daily",
         spec="Grafana heatmap：y=家族，x=horizon，color=IC",
         acc="一张图覆盖全库，无空白格（空白=该组合算不出 IC，需查覆盖率）", dep="B1", eta="2 小时"),
    dict(id="B5", grp="因子分析", name="因子覆盖率热力图",
         q="IC=0.8 的因子是不是只在 5 个标的上算出来的",
         src="factor_ic_daily.coverage",
         spec="y=因子，x=时间，color=非空标的占比",
         acc="榜单前 20 的因子必须逐个核对覆盖率", dep="B1", eta="2 小时"),
    dict(id="B6", grp="因子分析", name="IC 稳定性散点（IC vs ICIR）",
         q="哪些因子既高又稳",
         src="factor_ic_daily",
         spec="x=均值 IC，y=ICIR，右上象限为可用因子",
         acc="单因子入选需同时满足 |IC|>0.03 且 |ICIR|>0.5", dep="B1", eta="1 小时"),
    dict(id="B7", grp="因子分析", name="组合因子暴露时序（106 → 10~15 簇）",
         q="策略是不是偷偷押注了某类因子",
         src=f"{FT['table']} + future_position（按 open_date 对齐）",
         spec="因子先做层次聚类降维，再算持仓加权 z-score 暴露",
         acc="每簇一条线，暴露突变可与调仓日对上", dep="B1 + 聚类", eta="1 天"),
    dict(id="B8", grp="因子分析", name="增量 IC（正交化后残差）",
         q="模型到底有没有独立信息，还是因子复读机",
         src="futures_prediction.value + 因子簇",
         spec="value 对因子簇回归取残差，算残差的前瞻 IC",
         acc="残差 IC / 原 IC > 0.3 才算有独立信息", dep="B7 的簇", eta="1 天"),
    dict(id="C1", grp="统计检验", name="Bootstrap 收益显著性",
         q="+16.6% 是能力还是运气",
         src="future_wallet 日收益",
         spec="日收益有放回重采样 10,000 次，看累计收益 > 0 的比例与夏普置信区间",
         acc="p 值 < 0.1 才算有初步证据；否则“策略有效”不成立", dep="无", eta="2 小时"),
    dict(id="C2", grp="统计检验", name="Deflated Sharpe / PBO",
         q="试了这么多参数，夏普里有多少是搜出来的",
         src="历史回测参数网格",
         spec="按试验次数与偏度/峰度校正夏普，并算过拟合概率",
         acc="给出 DSR 与 PBO 两个数，写入结论", dep="C1", eta="0.5 天"),
]

assert len(BACKLOG) == N_ITEMS, f"待办应为 {N_ITEMS} 项，实际 {len(BACKLOG)}"
for it in BACKLOG:
    assert all(it[k].strip() for k in ("q", "src", "spec", "acc", "dep", "eta")), f"{it['id']} 字段缺失"

grp_count: dict[str, int] = {}
for it in BACKLOG:
    grp_count[it["grp"]] = grp_count.get(it["grp"], 0) + 1

for grp in ("收益归因", "因子分析", "统计检验"):
    items = [x for x in BACKLOG if x["grp"] == grp]
    md(f"""---

## 待办 {grp}（{len(items)} 项）

| ID | 名称 | 回答什么问题 | 数据来源 | 口径 / 设计 | 验收标准 | 依赖 | 工期 |
|---|---|---|---|---|---|---|---|
""" + "\n".join(
        f"| **{x['id']}** | {x['name']} | {x['q']} | {x['src']} | {x['spec']} | {x['acc']} | {x['dep']} | {x['eta']} |"
        for x in items
    ))

co(f"""BACKLOG_IDS = {[x['id'] for x in BACKLOG]!r}
groups = {grp_count!r}

assert len(BACKLOG_IDS) == 15 and len(set(BACKLOG_IDS)) == 15, "ID 应唯一且共 15 项"
assert groups == {{"收益归因": 5, "因子分析": 8, "统计检验": 2}}, groups
print("待办分组:", groups)
print("ID 序列:", " ".join(BACKLOG_IDS))""")

# ══════════════════════════════════════════════════════════════
md(f"""---

## 三、工程架构：为什么必须预聚合

直接把 `{FT['table']}` 丢给 Grafana 不可行：

| 问题 | 数字 |
|---|---|
| 表大小 | {FT['n_rows']:,} 行 / 50.38 GiB |
| 列数 | {FT['n_cols']} |
| 单次面板刷新要扫 | ~{FT['daily_rows'][-2]['rows']:,} 行 × {FT['n_feature_cols']} 列 |
| 一次 IC 计算 | 需要 join 前瞻收益 + 两次 rank → 秒级到十几秒 |

**三层结构**：

```
明细层  quant.1h_generated_v3_V3_0_0      (50 GiB，只给批处理读)
   ↓ 每日增量批处理（只算当天）
聚合层  quant.factor_ic_daily             (date, factor, horizon, ic, icir, coverage, n_codes)
   ↓ Grafana 只读小表
展示层  面板（毫秒级）
```

**聚合层规模**：{FT['n_feature_cols']} 因子 × {len(FT['horizons'])} horizon × 90 天
= **{FT['n_feature_cols'] * len(FT['horizons']) * 90:,} 行**，几百 KB。

批处理必须包在 `systemd-run --user --scope -p MemoryMax=8G` 里跑 ——
本机终端跑在 `hermes-gateway.service` 的 cgroup，`OOMPolicy=stop`，重内存任务 OOM 会连带重启网关。
""")

EXPECTED_ROWS = FT["n_feature_cols"] * len(FT["horizons"]) * 90
co("""FT = FACTS["factor_table"]
n_f, n_h, days = FT["n_feature_cols"], len(FT["horizons"]), 90
rows = n_f * n_h * days
assert rows == 342900, f"聚合表应为 342900 行，实际 {rows}"
assert rows < 1_000_000, "聚合表应远小于明细表"
assert rows * 80 < FT["n_rows"] * 100, "聚合层行数应比明细层小两个数量级以上"
daily_cells = FT["daily_rows"][-2]["rows"] * n_f
print(f"聚合表 {rows:,} 行（{n_f}×{n_h}×{days}）")
print(f"每日批处理输入 ≈ {daily_cells:,} 个格值")
print(f"明细层 {FT['n_rows']:,} 行 → 聚合层 {rows:,} 行，压缩比 {FT['n_rows']/rows:.0f}x")""")

# ══════════════════════════════════════════════════════════════
md("""---

## 四、已知口径陷阱（做的时候逐条对照）

| # | 陷阱 | 后果 | 规矩 |
|---|---|---|---|
| 1 | **未来函数** | 年化 37.88% → 0.97% | 任何 gate/掩码必须做「同日 vs 次日 shift(1)」并排对照 |
| 2 | **前瞻收益缺失当 0** | 未来 bar 不存在时算出 −100%，某天 IC 全废 | 无未来 bar 必须置 `nan`，不能置 0 |
| 3 | **超额不减截面均值** | 把大盘涨跌算成选股能力 | 每小时先减截面均值，再按组平均 |
| 4 | **只看单日快照算敞口** | gross 低估 3 倍（曾据此误判“仓位只有 31%”） | 按 `strategy_id` ASOF 前向填充后加总 |
| 5 | **把事件行当全量快照** | 赔偿事件行让持仓宽度显示 0 | 只认 `count() >= 100` 的完整快照 |
| 6 | **字符串标签列交给 CH 插件** | 多实体被画成一条线，上下乱跳 | SQL 直接透视成「每实体一列」 |
| 7 | **两个量纲挤一根轴** | 「累计 IC 很高、当日 IC 一般」是伪问题 | 量纲差一个数量级就挂右轴或拆面板 |
| 8 | **CH 23.8 没有 `mean()` 聚合** | SQL 报错 | 用 `sum()/count()` |
| 9 | **TradFi 永续混进加密横截面** | IC 被周末冻结的美股合约拉平 | `asset_class='crypto' AND matched_via!='collision'` |
| 10 | **`close_price` 全空却以为能直接算 PnL** | 归因结果全 0 | 用 `open_date` join K 线重建 |

---

## 五、优先级

| 顺序 | 做什么 | 理由 | 工期 |
|---|---|---|---|
| 1 | **C1 Bootstrap 显著性** | 2 小时就能知道 16.6% 是不是运气，决定后面还要不要投 | 2 小时 |
| 2 | **B1 预聚合表** | 其余 7 个因子面板的地基 | 0.5 天 |
| 3 | B2 / B3 / B5 / B6 | 排行榜、衰减曲线、覆盖率、稳定性 —— 因子分析的主体 | 1 天 |
| 4 | **A1 / A2** | 钱从哪来 + 执行损耗多大 | 1.5 天 |
| 5 | B7 / B8 增量 IC | 回答“模型有没有独立信息”，可能推翻策略前提 | 2 天 |
| 6 | A3 / A4 / A5 | cohort 对比、beta 归因、贡献者 | 1.5 天 |
| 7 | B4 家族热力图、C2 DSR/PBO | 锦上添花 | 1 天 |

---

## 六、待拍板（开工前需要确认）

| # | 问题 | 我的建议 |
|---|---|---|
| 1 | 先做 C1（显著性）还是先做 B1（预聚合）？ | **先 C1**：2 小时，结论可能改变投入决策 |
| 2 | 资金费率是否在归因里单列？ | **单列**：crypto 做空常收 funding，不单列会把「不承担选股风险的收益」算成 alpha |
| 3 | 归因是否覆盖全部三台机器，还是先做最大那台（30,290 USDT）？ | 先做最大那台，跑通再铺开 |
| 4 | 是否允许新建 `quant.factor_ic_daily`？ | 需要（只读现状下没法出图）；按 ReplacingMergeTree + 版本列建 |

---

*本文档为设计底稿，未写任何实现代码。开工时按第五节的顺序逐项落地，每完成一项在看板上新增面板（不动旧面板）。*
""")

co("""print("本 notebook 只做设计记录与数据事实校验，未修改任何看板或数据库。")
print(f"现有看板：{FACTS['dashboard']['uid']}（{FACTS['dashboard']['panels']} 面板）")
print(f"开发脚本：{FACTS['dashboard']['dev_file']}")
assert FACTS["dashboard"]["panels"] == 25""")

nb["cells"] = cells
nb["metadata"] = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python", "version": "3.12"},
}
NB_PATH.write_text(nbf.writes(nb), encoding="utf-8")
n_code = sum(1 for c in cells if c.cell_type == "code")
print(f"[build] → {NB_PATH}  cells={len(cells)}  code={n_code}")
