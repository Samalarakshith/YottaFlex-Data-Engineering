"""Transformation engine providing deduplication, standardization, and derived analytical attributes."""

import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from dateutil import parser as date_parser

from src.pipeline.schemas import FailureCode, QuarantineRecord


def deduplicate_records(
    records: List[Dict],
    key_field: str,
    timestamp_field: str = "updated_at",
    policy: str = "latest_wins",
    run_id: str = "",
    batch_id: str = "batch_1",
    table_name: str = "",
) -> Tuple[List[Dict], List[QuarantineRecord], int]:
    """Deduplicates records based on primary key and specified policy.

    Args:
        records: List of record dictionaries.
        key_field: Primary key column name ('customer_id' or 'order_id').
        timestamp_field: Timestamp field to evaluate recency.
        policy: 'latest_wins' | 'reject_all' | 'keep_first'.
        run_id: Run ID for audit tracking.
        batch_id: Batch ID.
        table_name: Name of table for quarantine logging.

    Returns:
        Tuple of (deduplicated_records, quarantined_records, duplicate_count).
    """
    if not records:
        return [], [], 0

    now_iso = datetime.now(timezone.utc).isoformat()

    if policy == "reject_all":
        # Any key appearing more than once is quarantined
        key_counts: Dict[str, int] = {}
        for r in records:
            k = r[key_field]
            key_counts[k] = key_counts.get(k, 0) + 1

        deduped: List[Dict] = []
        quarantined: List[QuarantineRecord] = []
        dup_count = 0

        for r in records:
            k = r[key_field]
            if key_counts[k] > 1:
                dup_count += 1
                quarantined.append(
                    QuarantineRecord(
                        quarantine_id=str(uuid.uuid4()),
                        run_id=run_id,
                        batch_id=batch_id,
                        source_table=table_name,
                        source_file=r.get("_source_file", ""),
                        source_row_number=r.get("_source_row_number", 0),
                        record_id=k,
                        failure_code=FailureCode.DUPLICATE_KEY_QUARANTINED.value,
                        failure_reason=f"Duplicate primary key '{k}' found in batch (policy=reject_all)",
                        raw_payload=r.get("_raw_payload", json.dumps(r)),
                        rejected_at=now_iso,
                    )
                )
            else:
                deduped.append(r)

        return deduped, quarantined, dup_count

    # Default policy: latest_wins (or keep_first)
    groups: Dict[str, List[Dict]] = {}
    for r in records:
        k = r[key_field]
        if k not in groups:
            groups[k] = []
        groups[k].append(r)

    deduped = []
    dup_count = 0

    for k, group in groups.items():
        if len(group) == 1:
            deduped.append(group[0])
        else:
            dup_count += (len(group) - 1)
            if policy == "keep_first":
                deduped.append(group[0])
            else:
                # latest_wins: sort by timestamp_field descending
                def get_ts(rec: Dict) -> datetime:
                    val = rec.get(timestamp_field)
                    try:
                        return date_parser.parse(val)
                    except Exception:
                        return datetime.min.replace(tzinfo=timezone.utc)

                sorted_group = sorted(group, key=get_ts, reverse=True)
                deduped.append(sorted_group[0])

    return deduped, [], dup_count


def transform_customers(
    customers: List[Dict],
    run_id: str,
) -> List[Dict]:
    """Transforms and enriches customer records for target dim_customers curated table."""
    now_iso = datetime.now(timezone.utc).isoformat()
    curated: List[Dict] = []

    for c in customers:
        first = c.get("first_name", "").strip()
        last = c.get("last_name", "").strip()
        full_name = f"{first} {last}".strip()

        curated.append({
            "customer_id": c["customer_id"],
            "first_name": first,
            "last_name": last,
            "full_name": full_name,
            "email": c.get("email", "").strip().lower(),
            "phone": c.get("phone", "").strip(),
            "address": c.get("address", "").strip(),
            "city": c.get("city", "").strip(),
            "state": c.get("state", "").strip().upper(),
            "postal_code": c.get("postal_code", "").strip(),
            "country": c.get("country", "US").strip().upper(),
            "registration_date": c["registration_date"],
            "updated_at": c["updated_at"],
            "is_active": True,
            "source_file": c.get("_source_file", ""),
            "run_id": run_id,
            "ingested_at": now_iso,
        })

    return curated


def transform_orders(
    orders: List[Dict],
    run_id: str,
) -> List[Dict]:
    """Transforms and enriches order records for target fct_orders curated table."""
    now_iso = datetime.now(timezone.utc).isoformat()
    curated: List[Dict] = []

    for o in orders:
        total = float(o["total_amount"])
        items = int(o["item_count"])
        avg_item_price = round(total / items, 2) if items > 0 else total

        curated.append({
            "order_id": o["order_id"],
            "customer_id": o["customer_id"],
            "order_date": o["order_date"],
            "order_status": o["order_status"].strip().upper(),
            "total_amount": round(total, 2),
            "currency": o.get("currency", "USD").strip().upper(),
            "payment_method": o.get("payment_method", "credit_card").strip().lower(),
            "item_count": items,
            "average_item_price": avg_item_price,
            "updated_at": o["updated_at"],
            "source_file": o.get("_source_file", ""),
            "run_id": run_id,
            "ingested_at": now_iso,
        })

    return curated
