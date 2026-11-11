"""Quarantine Reprocessing and Late-Arriving Data Backfill Engine.

Scans quarantine records for resolvable issues (e.g. orphan orders whose parent customer
has now arrived in dim_customers) and safely promotes them into curated tables.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple
import duckdb
from tabulate import tabulate

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.schemas import FailureCode
from src.pipeline.transformer import transform_orders
from src.pipeline.publisher import TargetPublisher


def reprocess_orphan_orders(db_path: Path | str, run_id: str) -> Tuple[int, float]:
    """Evaluates quarantined orphan orders against dim_customers and promotes resolved records.

    Returns:
        Tuple of (promoted_order_count, promoted_revenue_sum).
    """
    publisher = TargetPublisher(
        db_path=db_path,
        curated_dir=Path(db_path).parent,
        quarantine_dir=Path(db_path).parent.parent / "quarantine",
    )

    now_iso = datetime.now(timezone.utc).isoformat()

    with duckdb.connect(str(db_path)) as con:
        # 1. Fetch current known customer IDs
        known_cust_ids = set(
            row[0] for row in con.execute("SELECT customer_id FROM dim_customers").fetchall()
        )

        # 2. Fetch all quarantined orphan orders
        orphan_rows = con.execute("""
            SELECT quarantine_id, record_id, raw_payload, source_file, source_row_number
            FROM quarantine_records
            WHERE failure_code = ?
        """, [FailureCode.ORPHAN_FOREIGN_KEY.value]).fetchall()

        promoted_orders_raw: List[Dict] = []
        resolved_quarantine_ids: List[str] = []

        for q_id, ord_id, payload_str, src_file, row_num in orphan_rows:
            try:
                # Check if payload is json or csv-like
                if payload_str.startswith("{"):
                    row_dict = json.loads(payload_str)
                else:
                    # CSV line
                    parts = [p.strip() for p in payload_str.split(",")]
                    # order header: order_id, customer_id, order_date, order_status, total_amount, currency, payment_method, item_count, updated_at
                    if len(parts) >= 9:
                        row_dict = {
                            "order_id": parts[0],
                            "customer_id": parts[1],
                            "order_date": parts[2],
                            "order_status": parts[3],
                            "total_amount": float(parts[4]),
                            "currency": parts[5],
                            "payment_method": parts[6],
                            "item_count": int(parts[7]),
                            "updated_at": parts[8],
                        }
                    else:
                        continue

                cust_id = row_dict.get("customer_id")
                if cust_id and cust_id in known_cust_ids:
                    row_dict["_source_file"] = src_file
                    row_dict["_source_row_number"] = row_num
                    promoted_orders_raw.append(row_dict)
                    resolved_quarantine_ids.append(q_id)

            except Exception:
                continue

        if not promoted_orders_raw:
            return 0, 0.0

        # Transform orders
        curated_promoted = transform_orders(promoted_orders_raw, run_id=run_id)

        # Upsert into fct_orders
        publisher.publish_orders(curated_promoted)

        # Remove or mark resolved quarantine records
        placeholders = ",".join(["?"] * len(resolved_quarantine_ids))
        con.execute(f"DELETE FROM quarantine_records WHERE quarantine_id IN ({placeholders})", resolved_quarantine_ids)

        # Refresh analytical marts
        publisher.refresh_analytics_marts(run_id=run_id)
        publisher.export_files(export_parquet=True, export_csv=True)

        total_rev = sum(o["total_amount"] for o in curated_promoted)
        return len(curated_promoted), round(total_rev, 2)


def main():
    parser = argparse.ArgumentParser(description="Reprocess and backfill quarantined records.")
    parser.add_argument("--db", type=str, default="data/curated/warehouse.duckdb", help="Path to DuckDB warehouse")
    parser.add_argument("--run-id", type=str, default=f"backfill_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}")

    args = parser.parse_args()

    print(f"\n========================================================")
    print(f"       STARTING QUARANTINE BACKFILL / REPROCESSING")
    print(f"========================================================")
    print(f"Database: {args.db}")
    print(f"Run ID:   {args.run_id}")

    count, rev = reprocess_orphan_orders(db_path=args.db, run_id=args.run_id)

    results_table = [
        ["Promoted Orphan Orders", count],
        ["Promoted Order Revenue", f"${rev:,.2f}"],
        ["Target Updated", "fct_orders, agg_customer_metrics"],
        ["Quarantine Status", f"{count} records cleared from quarantine_records"],
    ]
    print("\n" + tabulate(results_table, headers=["Metric", "Result"], tablefmt="fancy_grid"))
    print("\nQuarantine backfill finished successfully.\n")


if __name__ == "__main__":
    main()
