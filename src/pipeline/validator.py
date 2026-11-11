"""Validation engine for customers and orders enforcing schema types, business rules, and referential integrity."""

import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from dateutil import parser as date_parser

from src.pipeline.schemas import (
    ALLOWED_ORDER_STATUSES,
    CUSTOMER_REQUIRED_COLUMNS,
    EMAIL_REGEX,
    ORDER_REQUIRED_COLUMNS,
    FailureCode,
    QuarantineRecord,
)


def _parse_iso_timestamp(ts_str: str) -> Optional[datetime]:
    """Attempts to parse timestamp into a timezone-aware UTC datetime."""
    if not ts_str or not ts_str.strip():
        return None
    try:
        dt = date_parser.parse(ts_str.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def validate_customers(
    raw_records: List[Dict[str, str]],
    run_id: str,
    batch_id: str = "batch_1",
) -> Tuple[List[Dict], List[QuarantineRecord]]:
    """Validates customer records against schema types and domain business rules."""
    valid_records: List[Dict] = []
    quarantined: List[QuarantineRecord] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for row in raw_records:
        row_num = int(row.get("_source_row_number", 0))
        source_file = row.get("_source_file", "")
        raw_payload = row.get("_raw_payload", json.dumps(row))
        cust_id = row.get("customer_id", "").strip()

        # Check primary key nullability
        if not cust_id:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="customers",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=None,
                    failure_code=FailureCode.NULL_PRIMARY_KEY.value,
                    failure_reason="Customer primary key 'customer_id' is null or empty",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Check missing required fields
        missing_fields = [
            f for f in CUSTOMER_REQUIRED_COLUMNS
            if f != "customer_id" and (f not in row or not row[f].strip())
        ]
        if missing_fields:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="customers",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=cust_id,
                    failure_code=FailureCode.MISSING_REQUIRED_FIELD.value,
                    failure_reason=f"Missing required customer fields: {missing_fields}",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Validate email format
        email = row.get("email", "").strip()
        if not EMAIL_REGEX.match(email):
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="customers",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=cust_id,
                    failure_code=FailureCode.INVALID_EMAIL_FORMAT.value,
                    failure_reason=f"Email address '{email}' violates standard RFC format",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Validate registration_date
        reg_dt = _parse_iso_timestamp(row.get("registration_date", ""))
        if not reg_dt:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="customers",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=cust_id,
                    failure_code=FailureCode.INVALID_TIMESTAMP.value,
                    failure_reason=f"registration_date '{row.get('registration_date')}' is not a valid ISO-8601 timestamp",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Validate updated_at
        upd_dt = _parse_iso_timestamp(row.get("updated_at", ""))
        if not upd_dt:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="customers",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=cust_id,
                    failure_code=FailureCode.INVALID_TIMESTAMP.value,
                    failure_reason=f"updated_at '{row.get('updated_at')}' is not a valid ISO-8601 timestamp",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        valid_records.append({
            "customer_id": cust_id,
            "first_name": row.get("first_name", "").strip(),
            "last_name": row.get("last_name", "").strip(),
            "email": email.lower(),
            "phone": row.get("phone", "").strip(),
            "address": row.get("address", "").strip(),
            "city": row.get("city", "").strip(),
            "state": row.get("state", "").strip().upper(),
            "postal_code": row.get("postal_code", "").strip(),
            "country": row.get("country", "US").strip().upper(),
            "registration_date": reg_dt.isoformat(),
            "updated_at": upd_dt.isoformat(),
            "_source_row_number": row_num,
            "_source_file": source_file,
            "_raw_payload": raw_payload,
        })

    return valid_records, quarantined


def validate_orders(
    raw_records: List[Dict[str, str]],
    run_id: str,
    batch_id: str = "batch_1",
    min_amount: float = 0.01,
    max_amount: float = 1_000_000.0,
    min_items: int = 1,
) -> Tuple[List[Dict], List[QuarantineRecord]]:
    """Validates order records against schema types and financial/status business rules."""
    valid_records: List[Dict] = []
    quarantined: List[QuarantineRecord] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for row in raw_records:
        row_num = int(row.get("_source_row_number", 0))
        source_file = row.get("_source_file", "")
        raw_payload = row.get("_raw_payload", json.dumps(row))
        ord_id = row.get("order_id", "").strip()
        cust_id = row.get("customer_id", "").strip()

        # Check order_id primary key nullability
        if not ord_id:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=None,
                    failure_code=FailureCode.NULL_PRIMARY_KEY.value,
                    failure_reason="Order primary key 'order_id' is null or empty",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Check customer_id foreign key presence
        if not cust_id:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=ord_id,
                    failure_code=FailureCode.MISSING_REQUIRED_FIELD.value,
                    failure_reason="Order foreign key 'customer_id' is missing or empty",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Check missing required fields
        missing_fields = [
            f for f in ORDER_REQUIRED_COLUMNS
            if f not in ("order_id", "customer_id") and (f not in row or not str(row[f]).strip())
        ]
        if missing_fields:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=ord_id,
                    failure_code=FailureCode.MISSING_REQUIRED_FIELD.value,
                    failure_reason=f"Missing required order fields: {missing_fields}",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Validate total_amount numeric and boundary rules
        try:
            amount = float(row.get("total_amount", ""))
        except (ValueError, TypeError):
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=ord_id,
                    failure_code=FailureCode.INVALID_NUMERIC.value,
                    failure_reason=f"total_amount '{row.get('total_amount')}' cannot be converted to numeric float",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        if amount < min_amount:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=ord_id,
                    failure_code=FailureCode.NEGATIVE_OR_ZERO_AMOUNT.value,
                    failure_reason=f"total_amount {amount:.2f} is below minimum required {min_amount:.2f}",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        if amount > max_amount:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=ord_id,
                    failure_code=FailureCode.EXCEEDS_MAX_AMOUNT.value,
                    failure_reason=f"total_amount {amount:.2f} exceeds maximum allowed {max_amount:.2f}",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Validate item_count integer and positive
        try:
            items = int(row.get("item_count", ""))
            if items < min_items:
                raise ValueError("Item count below minimum")
        except (ValueError, TypeError):
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=ord_id,
                    failure_code=FailureCode.INVALID_ITEM_COUNT.value,
                    failure_reason=f"item_count '{row.get('item_count')}' must be an integer >= {min_items}",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Validate order_status
        status_clean = row.get("order_status", "").strip().upper()
        if status_clean not in ALLOWED_ORDER_STATUSES:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=ord_id,
                    failure_code=FailureCode.INVALID_ORDER_STATUS.value,
                    failure_reason=f"order_status '{status_clean}' not in allowed statuses: {sorted(list(ALLOWED_ORDER_STATUSES))}",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Validate order_date timestamp
        ord_dt = _parse_iso_timestamp(row.get("order_date", ""))
        if not ord_dt:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=ord_id,
                    failure_code=FailureCode.INVALID_TIMESTAMP.value,
                    failure_reason=f"order_date '{row.get('order_date')}' is not a valid ISO-8601 timestamp",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        # Validate updated_at timestamp
        upd_dt = _parse_iso_timestamp(row.get("updated_at", ""))
        if not upd_dt:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=source_file,
                    source_row_number=row_num,
                    record_id=ord_id,
                    failure_code=FailureCode.INVALID_TIMESTAMP.value,
                    failure_reason=f"updated_at '{row.get('updated_at')}' is not a valid ISO-8601 timestamp",
                    raw_payload=raw_payload,
                    rejected_at=now_iso,
                )
            )
            continue

        valid_records.append({
            "order_id": ord_id,
            "customer_id": cust_id,
            "order_date": ord_dt.isoformat(),
            "order_status": status_clean,
            "total_amount": round(amount, 2),
            "currency": row.get("currency", "USD").strip().upper(),
            "payment_method": row.get("payment_method", "credit_card").strip().lower(),
            "item_count": items,
            "updated_at": upd_dt.isoformat(),
            "_source_row_number": row_num,
            "_source_file": source_file,
            "_raw_payload": raw_payload,
        })

    return valid_records, quarantined


def validate_referential_integrity(
    orders: List[Dict],
    known_customer_ids: Set[str],
    run_id: str,
    batch_id: str = "batch_1",
) -> Tuple[List[Dict], List[QuarantineRecord]]:
    """Enforces referential integrity by checking order customer_id against known customer IDs.

    Args:
        orders: List of validated order dictionaries.
        known_customer_ids: Union of valid customer IDs from current batch and existing dim_customers.
        run_id: Pipeline run identifier.
        batch_id: Batch identifier.

    Returns:
        Tuple of (retained_valid_orders, orphan_quarantined_records).
    """
    retained_orders: List[Dict] = []
    quarantined: List[QuarantineRecord] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for ord_dict in orders:
        cust_id = ord_dict["customer_id"]
        if cust_id not in known_customer_ids:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table="orders",
                    source_file=ord_dict.get("_source_file", ""),
                    source_row_number=ord_dict.get("_source_row_number", 0),
                    record_id=ord_dict["order_id"],
                    failure_code=FailureCode.ORPHAN_FOREIGN_KEY.value,
                    failure_reason=f"Referential integrity failure: customer_id '{cust_id}' does not exist in dim_customers",
                    raw_payload=ord_dict.get("_raw_payload", json.dumps(ord_dict)),
                    rejected_at=now_iso,
                )
            )
        else:
            retained_orders.append(ord_dict)

    return retained_orders, quarantined
