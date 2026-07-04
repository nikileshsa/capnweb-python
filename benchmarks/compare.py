"""Merge python_baseline.json + ts_baseline.json into a side-by-side table.

    uv run python -m benchmarks.compare

Prints, per workload present in both, the Python and TS medians and the
Python/TS ratio (>1 = TS faster). Workloads only present on one side are
listed separately. This is descriptive only — it reads the two JSON files the
runners already wrote; it does not run any benchmark.
"""

from __future__ import annotations

import json
from pathlib import Path

_DIR = Path(__file__).parent / "results"

# Names that mean the same workload across the two harnesses.
_ALIASES = {
    "batch/1000_calls_msgport": "batch/1000_calls",
    "batch/1000_calls_pipe": "batch/1000_calls",
    "stream/64MiB_msgport": "stream/64MiB",
    "stream/64MiB_loopback": "stream/64MiB",
}


def _key(name: str) -> str:
    return _ALIASES.get(name, name)


def _load(p: Path) -> dict:
    return json.loads(p.read_text()) if p.exists() else {"results": [], "env": {}}


def main() -> None:
    py = _load(_DIR / "python_baseline.json")
    ts = _load(_DIR / "ts_baseline.json")

    py_map = {_key(r["name"]): r for r in py["results"]}
    ts_map = {_key(r["name"]): r for r in ts["results"]}

    print(f"Python env: {py['env'].get('cpu_brand','?')} | "
          f"{py['env'].get('python_impl','')} {py['env'].get('python_version','')}")
    print(f"TS env:     {ts['env'].get('cpu_brand','?')} | "
          f"node {ts['env'].get('node_version','')} v8 {ts['env'].get('v8_version','')}")
    print()
    hdr = f"{'workload':<40} {'Python median':>16} {'TS median':>16} {'PY/TS':>8}"
    print(hdr)
    print("-" * len(hdr))

    def fmt(ns: float) -> str:
        if ns < 1000:
            return f"{ns:.0f} ns"
        if ns < 1e6:
            return f"{ns/1e3:.2f} us"
        return f"{ns/1e6:.3f} ms"

    both = sorted(set(py_map) & set(ts_map))
    for k in both:
        p = py_map[k]["ns_median"]
        t = ts_map[k]["ns_median"]
        ratio = p / t if t else float("inf")
        print(f"{k:<40} {fmt(p):>16} {fmt(t):>16} {ratio:>7.1f}x")

    py_only = sorted(set(py_map) - set(ts_map))
    if py_only:
        print("\nPython-only workloads:")
        for k in py_only:
            print(f"  {k:<40} {fmt(py_map[k]['ns_median']):>16}  {py_map[k].get('extra',{})}")


if __name__ == "__main__":
    main()
