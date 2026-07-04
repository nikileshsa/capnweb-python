"""Run the full Python baseline suite and emit JSON + a sorted table.

Usage:
    uv run python -m benchmarks.run_all             # all groups
    uv run python -m benchmarks.run_all serialize   # subset by keyword
    uv run python -m benchmarks.run_all --json OUT.json

Writes ``benchmarks/results/python_baseline.json`` by default (machine/env
metadata + every Result) so the optimization pass can diff before/after.
"""

from __future__ import annotations

import json
import platform
import sys
import time
from pathlib import Path

from benchmarks._harness import Result, summarize

_RESULTS_DIR = Path(__file__).parent / "results"


def _env() -> dict:
    import capnweb

    info = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "python_version": sys.version.split()[0],
        "python_impl": platform.python_implementation(),
        "platform": platform.platform(),
        "processor": platform.processor(),
        "machine": platform.machine(),
        "capnweb_version": getattr(capnweb, "__version__", "?"),
    }
    try:
        import subprocess

        info["cpu_brand"] = subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
        ).strip()
        info["ncpu"] = subprocess.check_output(
            ["sysctl", "-n", "hw.ncpu"], text=True
        ).strip()
    except Exception:
        pass
    return info


def collect(filter_kw: str | None = None) -> list[Result]:
    from benchmarks import (
        bench_serialize,
        bench_rpc,
        bench_streams,
        bench_tables,
    )

    modules = {
        "serialize": bench_serialize,
        "rpc": bench_rpc,
        "streams": bench_streams,
        "tables": bench_tables,
    }
    results: list[Result] = []
    for key, mod in modules.items():
        if filter_kw and filter_kw not in key:
            continue
        print(f"[run_all] running {key} ...", file=sys.stderr, flush=True)
        results += mod.run()
    return results


def main(argv: list[str]) -> None:
    filter_kw = None
    out_path = _RESULTS_DIR / "python_baseline.json"
    args = list(argv)
    if "--json" in args:
        i = args.index("--json")
        out_path = Path(args[i + 1])
        del args[i : i + 2]
    if args:
        filter_kw = args[0]

    results = collect(filter_kw)

    # Sort so the table groups cleanly.
    results.sort(key=lambda r: (r.group, r.name))
    print(summarize(results))
    print()
    for r in results:
        if r.extra:
            print(f"  {r.name}: {r.extra}")

    _RESULTS_DIR.mkdir(exist_ok=True)
    payload = {"env": _env(), "results": [r.as_dict() for r in results]}
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"\n[run_all] wrote {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1:])
