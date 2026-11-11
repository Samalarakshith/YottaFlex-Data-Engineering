"""Command-line reconciliation tool for pipeline audit verification."""

import argparse
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.reconciler import run_reconciliation


def main():
    parser = argparse.ArgumentParser(description="Reconcile pipeline inputs, curated outputs, and quarantine.")
    parser.add_argument("--db", type=str, default="data/curated/warehouse.duckdb", help="Path to DuckDB warehouse")
    parser.add_argument("--run-id", type=str, default=None, help="Specific run ID to reconcile (defaults to latest)")

    args = parser.parse_args()

    report = run_reconciliation(db_path=args.db, latest_run_id=args.run_id)
    print(report.print_report())

    sys.exit(0 if report.all_reconciled else 1)


if __name__ == "__main__":
    main()
