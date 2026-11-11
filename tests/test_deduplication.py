"""Unit tests for deduplication strategies and recency resolution."""

from src.pipeline.schemas import FailureCode
from src.pipeline.transformer import deduplicate_records


def test_dedup_latest_wins():
    records = [
        {
            "customer_id": "CUST-001",
            "email": "old@example.com",
            "updated_at": "2024-01-01T10:00:00Z",
            "_source_row_number": 2,
        },
        {
            "customer_id": "CUST-001",
            "email": "latest@example.com",
            "updated_at": "2024-01-05T10:00:00Z",
            "_source_row_number": 3,
        },
        {
            "customer_id": "CUST-002",
            "email": "other@example.com",
            "updated_at": "2024-01-02T10:00:00Z",
            "_source_row_number": 4,
        },
    ]

    deduped, quarantined, dup_count = deduplicate_records(
        records,
        key_field="customer_id",
        timestamp_field="updated_at",
        policy="latest_wins",
    )

    assert dup_count == 1
    assert len(deduped) == 2
    assert len(quarantined) == 0

    c1 = next(r for r in deduped if r["customer_id"] == "CUST-001")
    assert c1["email"] == "latest@example.com"


def test_dedup_out_of_order_latest_wins():
    # Newer record appears first in the list
    records = [
        {
            "order_id": "ORD-001",
            "total_amount": 100.0,
            "updated_at": "2024-02-10T12:00:00Z",
            "_source_row_number": 2,
        },
        {
            "order_id": "ORD-001",
            "total_amount": 50.0,
            "updated_at": "2024-01-01T12:00:00Z",  # Older arrives second
            "_source_row_number": 3,
        },
    ]

    deduped, quarantined, dup_count = deduplicate_records(
        records,
        key_field="order_id",
        timestamp_field="updated_at",
        policy="latest_wins",
    )

    assert dup_count == 1
    assert len(deduped) == 1
    assert deduped[0]["total_amount"] == 100.0


def test_dedup_reject_all_policy():
    records = [
        {
            "customer_id": "CUST-001",
            "email": "a@example.com",
            "updated_at": "2024-01-01T00:00:00Z",
            "_source_row_number": 2,
        },
        {
            "customer_id": "CUST-001",
            "email": "b@example.com",
            "updated_at": "2024-01-02T00:00:00Z",
            "_source_row_number": 3,
        },
        {
            "customer_id": "CUST-002",
            "email": "unique@example.com",
            "updated_at": "2024-01-01T00:00:00Z",
            "_source_row_number": 4,
        },
    ]

    deduped, quarantined, dup_count = deduplicate_records(
        records,
        key_field="customer_id",
        policy="reject_all",
        table_name="customers",
    )

    assert dup_count == 2
    assert len(deduped) == 1
    assert deduped[0]["customer_id"] == "CUST-002"
    assert len(quarantined) == 2
    for q in quarantined:
        assert q.failure_code == FailureCode.DUPLICATE_KEY_QUARANTINED.value
