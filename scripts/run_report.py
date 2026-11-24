"""CLI runner: print and export the two-ended flow reconciliation report.

Usage:
    python -I scripts/run_report.py [--edge-type Line] [--data-type actual]
                                    [--series flow] [--out-dir reports]

Emits:
    * <out>/differences.csv  — paired, both-present comparisons
    * <out>/coverage.csv    — unpaired, one-sided readings
    * <out>/missing_endpoints.csv — endpoints with no matching series
and prints the tables plus a summary to stdout.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow `python scripts/run_report.py` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import energydb as edb  # noqa: E402
import polars as pl  # noqa: E402
from reconcile import ReconciliationReport, reconcile_edges  # noqa: E402


def _print_report(report: ReconciliationReport) -> None:
    print("=" * 78)
    print("EDGE TOPOLOGY IDENTITIES USED")
    print("=" * 78)
    for i in report.identities:
        label = f"{i.edge_type} {i.edge_name!r} ({i.edge_uuid})"
        print(f"  [{i.role:4}] {i.node_path:24} on {label}  series={i.data_type}/{i.series_name}")

    print()
    print("=" * 78)
    print(f"DIFFERENCES (both ends present, aligned on valid_time) — {report.differences.height} rows")
    print("=" * 78)
    with pl.Config(tbl_rows=100, tbl_cols=20, tbl_width_chars=120):
        print(report.differences)

    print()
    print("=" * 78)
    print(f"COVERAGE (no counterpart on the other end) — {report.coverage.height} rows")
    print("=" * 78)
    with pl.Config(tbl_rows=100, tbl_cols=20, tbl_width_chars=120):
        print(report.coverage)

    print()
    print("=" * 78)
    print(f"MISSING ENDPOINTS (no matching series at all) — {report.missing_endpoints.height} rows")
    print("=" * 78)
    with pl.Config(tbl_rows=100, tbl_cols=20, tbl_width_chars=120):
        print(report.missing_endpoints)

    print()
    print("=" * 78)
    print("SUMMARY")
    print("=" * 78)
    for k, v in report.summary().items():
        print(f"  {k:24}: {v}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--edge-type", default="Line")
    ap.add_argument("--data-type", default="actual")
    ap.add_argument("--series", default="flow")
    ap.add_argument("--out-dir", default="reports")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    with edb.Client() as client:
        report = reconcile_edges(
            client,
            edge_type=args.edge_type,
            data_type=args.data_type,
            series_name=args.series,
        )

    diff_path = out / "differences.csv"
    cov_path = out / "coverage.csv"
    missing_path = out / "missing_endpoints.csv"
    report.differences.write_csv(diff_path)
    report.coverage.write_csv(cov_path)
    report.missing_endpoints.write_csv(missing_path)

    _print_report(report)
    print()
    print(f"wrote {diff_path}")
    print(f"wrote {cov_path}")
    print(f"wrote {missing_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
