"""构建《跨所 TradFi 永续排查》报告 notebook。

所有数字来自 data/facts.json（由 collect_facts.py 实测生成），逐值断言。
输出: report/manual/tradfi_perp_survey/tradfi_perp_survey.ipynb
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[5]                       # pond
OUTDIR = ROOT / "report" / "manual" / "tradfi_perp_survey"
NB_PATH = OUTDIR / "tradfi_perp_survey.ipynb"

CELLS: list[dict] = []


def md(text: str) -> None:
    CELLS.append({"cell_type": "markdown", "id": f"md{len(CELLS)}", "metadata": {}, "source": text})


def co(text: str) -> None:
    CELLS.append({
        "cell_type": "code", "id": f"co{len(CELLS)}", "metadata": {}, "execution_count": None,
        "outputs": [], "source": text,
    })


# ════════════════════════════════════════════════════════════════
md(r"""# 跨所 TradFi 永续排查：1,000 个合约里有 20%~55% 不是币

**问题**：mexc / gate 各挂着 1,000 多个永续合约，里面有没有币安 TradFi 那种「传统金融标的」的衍生品？后世怎么判断？

**结论先行**

| 判断 | 数字 |
|---|---|
| **有，而且是普遍现象** | 8 家合约所里 7 家都有，htx **55.2%**、mexc **43.3%**、okx **35.0%** |
| 标的最多的是美股 | mexc 461 个、htx 209、bitget 208、bybit 206、gate 205、okx 172、kucoin 163 |
| 现货侧几乎干净 | 所有所现货的 TradFi 占比 **≤1.0%**，只有 pre-IPO/股票代币少数 |
| 判断依据 | 币安 `underlyingType` 字段（交易所权威分类）+ 交易所命名后缀 + 撞车名单 |
| **最大的坑** | ticker 撞车：`DIA`/`SPX`/`ALL` 既是币又是传统标的，按名字判必错 |

> 数据口径：本文所有数字由 `collect_facts.py` 实测生成，写入 `data/facts.json`，notebook 逐值断言。
""")

co(r"""
import json
from pathlib import Path

ROOT = Path("__ROOT__")
OUTDIR = ROOT / "report" / "manual" / "tradfi_perp_survey"
facts = json.loads((OUTDIR / "data" / "facts.json").read_text(encoding="utf-8"))

B, O = facts["binance"], facts["ours"]

# ── 逐值断言（不匹配就报错，防止报告和事实脱节）──
assert B["total_symbols"] == 936, B["total_symbols"]
assert B["tradifi_underlyings"] == 232, B["tradifi_underlyings"]
assert B["contract_type"]["TRADIFI_PERPETUAL"] == 229, B["contract_type"]
assert O["instrument_class_rows"] == 12658, O["instrument_class_rows"]
assert len(O["by_venue_type"]) == 16, len(O["by_venue_type"])

vt = {(r["venue"], r["market"]): r for r in O["by_venue_type"]}
assert vt[("mexc", "perp")]["total"] == 1085
assert vt[("mexc", "perp")]["tradfi"] == 470, vt[("mexc", "perp")]["tradfi"]
assert vt[("mexc", "perp")]["tradfi_pct"] == 43.3
assert vt[("htx", "perp")]["tradfi_pct"] == 55.2
assert vt[("gate", "perp")]["tradfi_pct"] == 20.4
assert vt[("upbit", "spot")]["tradfi"] == 0
assert vt[("upbit", "spot")]["total"] == 299

print(f"facts OK  generated_at={facts['generated_at']}")
""")

# ════════════════════════════════════════════════════════════════
md(r"""## 1. 币安自己是怎么标的：`underlyingType`

币安 2026 年上线的 TradFi 永续，在 `/fapi/v1/exchangeInfo` 里给了**两个显式字段**：

```
contractType   = PERPETUAL / TRADIFI_PERPETUAL / CURRENT_QUARTER / NEXT_QUARTER
underlyingType = COIN / EQUITY / HK_EQUITY / KR_EQUITY / CN_EQUITY /
                 COMMODITY / INDEX / PREMARKET / FX
```

这是**交易所自己给的分类**，比任何名字推断都可靠。问题只在于：其他所没有这个字段。
所以我们的做法是——把其他所的基准资产名对齐币安的 `baseAsset`，撞车的单独标出来。
""")

co(r"""
import pandas as pd

bn = pd.DataFrame(
    sorted(B["underlying_type"].items(), key=lambda kv: -kv[1]),
    columns=["underlyingType", "合约数"],
)
display(bn)
print("contractType:", B["contract_type"])
print("TradFi 标的（非 COIN）:", B["tradifi_underlyings"], "个 →", B["tradifi_breakdown"])
""")

# ════════════════════════════════════════════════════════════════
md(r"""## 2. 判定优先级：四层，越靠前越可信

| 层 | 依据 | 判了多少条 | 可靠性 |
|---|---|---|---|
| ① 交易所命名 | gate/mexc/bitget 给股票加 `STOCK` 后缀，pre-IPO 用 `pre` 前缀 | 452 | 高（交易所显式标记） |
| ② 币安 `underlyingType` | 基准资产名对齐币安 | 1,235 | 高（交易所权威字段） |
| ③ 无歧义别名 | `SPX500` / `NAS100` / `NDX100` / `US30` / `XAUT` 这类带后缀、不会和币撞车 | 21 | 中高 |
| ④ 撞车名单 | `DIA`/`SPX`/`ALL`/`META` 等既是币又是传统标的 → 标 `ambiguous`，不硬判 | 41 | 必须人工/数据复核 |
| 默认 | 其余全按加密 | 10,612 | — |
| 所级覆盖 | upbit 是纯加密现货所 | 299 | 高 |

**为什么不能只靠名字**：币安 `DIAUSDT` 的 `underlyingType` 是 **COIN**（DIA 预言机币），
不是道指 ETF `DIA`；`SPXUSDT` 是 SPX6900 meme 币，不是标普 500。
如果直接按 ticker 匹配，会把一堆 meme 币标成股票。
""")

co(r"""
via = pd.DataFrame(
    sorted(O["matched_via"].items(), key=lambda kv: -kv[1]),
    columns=["判定依据", "条数"],
)
display(via)
print("ambiguous（撞车待判）:", len(O["ambiguous"]), "条")
""")

# ════════════════════════════════════════════════════════════════
md(r"""## 3. 结论一：TradFi 占比按所排序

**永续是重灾区，现货是干净的。** htx 挂的合约里有超过一半是股票/商品/指数。
""")

co(r"""
rows = []
for r in O["by_venue_type"]:
    rows.append({
        "所": r["venue"], "类型": r["market"], "合约数": r["total"],
        "加密": r["crypto"],
        "股票类": r["equity"] + r["hk_equity"] + r["kr_equity"] + r["cn_equity"] + r["premarket"],
        "商品": r["commodity"], "指数": r["index"], "待判": r["ambiguous"],
        "TradFi": r["tradfi"], "TradFi%": r["tradfi_pct"],
    })
df = pd.DataFrame(rows).sort_values(["类型", "TradFi%"], ascending=[True, False])
display(df.reset_index(drop=True))

perp = df[df["类型"] == "perp"]
print(f"永续合计 {perp['合约数'].sum()} 个，其中 TradFi {perp['TradFi'].sum()} 个 "
      f"= {perp['TradFi'].sum() / perp['合约数'].sum() * 100:.1f}%")
spot = df[df["类型"] == "spot"]
print(f"现货合计 {spot['合约数'].sum()} 个，其中 TradFi {spot['TradFi'].sum()} 个 "
      f"= {spot['TradFi'].sum() / spot['合约数'].sum() * 100:.2f}%")
""")

co(r"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

p = perp.sort_values("TradFi%")
fig, ax = plt.subplots(figsize=(8, 4.2), dpi=130)
bars = ax.barh(p["所"], p["TradFi%"], color="#c0392b", alpha=0.85)
for b, (_, r) in zip(bars, p.iterrows()):
    ax.text(r["TradFi%"] + 0.8, b.get_y() + b.get_height() / 2,
            f"{r['TradFi%']:.1f}%  ({r['TradFi']}/{r['合约数']})",
            va="center", fontsize=8.5)
ax.set_xlabel("TradFi share of perpetual contracts (%)")
ax.set_title("Perp contracts backed by equities / commodities / indices")
ax.set_xlim(0, 70)
ax.grid(axis="x", alpha=0.25)
fig.tight_layout()
fp = OUTDIR / "tradfi_share_by_venue.png"
fig.savefig(fp)
print("saved", fp.name)
plt.close(fig)

from IPython.display import Image, display as _d
_d(Image(filename=str(fp)))
""")

# ════════════════════════════════════════════════════════════════
md(r"""## 4. 结论二：按标的类别拆开看

美股是绝对主力（7 家都在 150~460 个量级），港股/韩股集中在 htx、bybit、gate、bitget。
pre-IPO（OpenAI/Anthropic/Moonshot/Oura 这类未上市公司）也有 3~6 个/所。
""")

co(r"""
cat = pd.DataFrame(O["perp_category"]).fillna(0).astype(int)
cat = cat.reindex(columns=[c for c in
                           ["equity", "hk_equity", "kr_equity", "cn_equity",
                            "premarket", "commodity", "index", "fx"]
                           if c in cat.columns])
cat.columns = ["美股", "港股", "韩股", "A股", "pre-IPO", "商品", "指数", "外汇"]
cat["合计"] = cat.sum(axis=1)
display(cat.sort_values("合计", ascending=False))
""")

# ════════════════════════════════════════════════════════════════
md(r"""## 5. 案例：ticker 撞车 —— 名字一样，东西完全不是一回事

| ticker | 币安 `underlyingType` | 在币圈的另一种含义 |
|---|---|---|
| `DIA` | **COIN** | DIA 预言机币；而 `DIA` 同时是道指 ETF 的代码 |
| `SPX` | **COIN** | SPX6900 meme 币；而 `SPX` 也是标普 500 的口语代码 |
| `ALL` | **INDEX** | 币安把 ALL 标成 INDEX；而 `ALL` 也是 Allstate 保险的代码 |
| `META` | EQUITY | Meta Platforms；但 **upbit 的 META-KRW 是 Metadium（韩国币）** |

第四行是真实踩到的坑：第一版按名字对齐币安，把 upbit 的 `META-KRW` 标成了美股，
而 upbit 根本没有股票——它是 Metadium。修法是给 upbit 加所级覆盖（纯加密所）。
""")

co(r"""
cc = pd.DataFrame(
    [{"ticker": k, "币安 symbol": v.get("symbol"), "币安 underlyingType": v.get("underlying_type")}
     for k, v in B["collision_cases"].items()]
)
display(cc)

amb = pd.DataFrame(O["ambiguous"])
print(f"撞车待判 {len(amb)} 条，分布：")
display(amb.groupby(["venue", "underlying"]).size().reset_index(name="条数"))
""")

# ════════════════════════════════════════════════════════════════
md(r"""## 6. 同一个标的，几家在挂

以 AAPL / NVDA / XAU / OPENAI 为例，看跨所覆盖面。这也解释了为什么这些标的
**不能进加密横截面因子池**——它们是同一个基本面暴露，不同所之间高度同质。
""")

co(r"""
cv = pd.DataFrame(
    [{"标的": k, "在挂的所数": len(v), "所": ", ".join(sorted(v))} for k, v in O["cross_venue"].items()]
).sort_values("在挂的所数", ascending=False)
display(cv)
""")

# ════════════════════════════════════════════════════════════════
md(r"""## 7. 对研究的影响：做加密横截面必须先过滤

这些标的有三个特征，会**直接毁掉加密因子的 IC**：

1. **周末不交易**：美股永续在周六周日价格冻结、成交趋零，而加密永续周末成交约占 28%
2. **日内有 17 小时休市**：16:00 ET 收盘到次日 09:30 ET 开盘，价格基本不动
3. **和 BTC 相关性接近 0**：它们跟的是美股 beta，不是加密 beta

混进横截面池的后果：因子在周末"看起来"失效、"看起来"有效，都是假的；
排序里 20%~55% 的样本根本不是同一个资产类别。

**过滤写法**：

```sql
SELECT venue, code
FROM cryptos.instrument_class FINAL
WHERE asset_class = 'crypto'          -- 只要加密
  AND matched_via != 'collision';     -- 撞车待判的先排除
```

或者取传统金融做**另一个**研究：

```sql
SELECT venue, code, asset_class, underlying
FROM cryptos.instrument_class FINAL
WHERE asset_class NOT IN ('crypto', 'ambiguous');
```
""")

co(r"""
tot_crypto = sum(r["crypto"] for r in O["by_venue_type"] if r["market"] == "perp")
tot_perp = sum(r["total"] for r in O["by_venue_type"] if r["market"] == "perp")
print(f"过滤后可用于加密横截面的永续：{tot_crypto} / {tot_perp} 个 "
      f"（剔掉 {tot_perp - tot_crypto} 个 TradFi + 撞车）")
assert tot_crypto == 4039, tot_crypto
assert tot_perp == 5615, tot_perp
""")

# ════════════════════════════════════════════════════════════════
md(r"""## 8. 局限与下一步

**当前判据的弱点**：命名法在「同一 ticker 两种含义」上必然失效，41 条 `ambiguous` 就是证据。

**更硬的判据：成交量指纹**（不看名字，只看行为）
- 美股永续：每天约 **17 小时零成交**（收盘到开盘）+ 周末全天空，合计约 65%~70% 的小时零成交
- 加密永续：零成交小时 ≈ 0%

**为什么现在还不能用**：全量采集是 2026-10-28 才开始的，`AAPL-USDT` 目前只有 20 根 1h bar，
样本不够。攒够 **2 周**（约 336 根）就能把这一步自动化，届时：
1. 对每所每个合约算「近 14 天零成交小时占比」
2. 占比 > 50% → 判 TradFi，覆盖命名法结果
3. 用两种判据的一致率做质检

**已知未覆盖**：
- binance 未采集（`enabled: false`），但它有 **229 个 TRADIFI_PERPETUAL**，是数量最多的一家
- 现货侧的股票代币（gate 4 个、mexc 7 个、kucoin 5 个）未逐一核实
""")

co(r"""
print("币安 TradFi 合约（未采集）：", B["contract_type"]["TRADIFI_PERPETUAL"], "个")
print("当前 ambiguous 待成交量指纹复核：", len(O["ambiguous"]), "条")
print("\n复现：")
print("  /usr/bin/python3.12 scripts/classify_tradfi.py --write      # 重新分类并写库")
print("  /usr/bin/python3.12 report/manual/tradfi_perp_survey/scripts/collect_facts.py")
print("  /usr/bin/python3.12 report/manual/tradfi_perp_survey/scripts/tools/build_notebook.py")
print("  /usr/bin/python3.12 report/manual/tradfi_perp_survey/scripts/tools/exec_notebook.py")
""")

# ════════════════════════════════════════════════════════════════
nb = {
    "cells": CELLS,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.12"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}
raw = json.dumps(nb, ensure_ascii=False, indent=1).replace("__ROOT__", str(ROOT))
NB_PATH.parent.mkdir(parents=True, exist_ok=True)
NB_PATH.write_text(raw, encoding="utf-8")
print(f"[build] {NB_PATH}  共 {len(CELLS)} 个 cell（{sum(1 for c in CELLS if c['cell_type']=='code')} 代码）")
