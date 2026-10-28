"""执行《跨所 TradFi 永续排查》notebook（原地跑到底，0 报错才算通过）。

用法: /usr/bin/python3.12 report/manual/tradfi_perp_survey/scripts/tools/exec_notebook.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import nbformat
from nbclient import NotebookClient

NB = Path(__file__).resolve().parents[2] / "tradfi_perp_survey.ipynb"

nb = nbformat.read(NB, as_version=4)
client = NotebookClient(
    nb,
    timeout=900,
    kernel_name="python3",
    resources={"metadata": {"path": str(NB.parent)}},
    allow_errors=False,
)
client.execute()

n_code = sum(1 for c in nb.cells if c.cell_type == "code")
n_out = sum(1 for c in nb.cells if c.cell_type == "code" and c.get("outputs"))
errors = [
    (i, o) for i, c in enumerate(nb.cells) if c.cell_type == "code"
    for o in c.get("outputs", []) if o.get("output_type") == "error"
]
nbformat.write(nb, NB)
print(f"[exec] code cells={n_code} outputs={n_out} errors={len(errors)}")
if errors:
    print(errors)
    sys.exit(1)
print("✅ 运行到底 0 报错")
