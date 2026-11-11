"""Unit tests for quarantine reprocessing and late-arriving parent backfill."""

from pathlib import Path
import duckdb
from src.pipeline.schemas import FailureCode, QuarantineRecord
from src.pipeline.publisher import TargetPublisher
from scripts.reprocess_quarantine import reprocess_orphan_orders


def test_reprocess_orphan_orders_when_parent_arrives(temp_workspace):
    db_file = temp_workspace["db_file"]
    publisher = TargetPublisher(
        db_path=db_file,
        curated_dir=temp_workspace["curated_dir"],
        quarantine_dir=temp_workspace["quarantine_dir"],
    )

    # 1. Quarantine an orphan order for non-existent customer CUST-888
    raw_csv_order = "ORD-999,CUST-888,2024-03-01T12:00:00Z,DELIVERED,175.50,USD,credit_card,2,2024-03-01T12:00:00Z"
    quarantined = [
        QuarantineRecord(
            quarantine_id="q-test-1",
            run_id="run-1",
            batch_id="b1",
            source_table="orders",
            source_file="orders.csv",
            source_row_number=10,
            record_id="ORD-999",
            failure_code=FailureCode.ORPHAN_FOREIGN_KEY.value,
            failure_reason="Customer not found",
            raw_payload=raw_csv_order,
            rejected_at="2024-03-01T12:00:00Z",
        )
    ]
    publisher.publish_quarantine(quarantined)

    # Verify order is not yet in fct_orders
    con = duckdb.connect(str(db_file), read_only=True)
    assert con.execute("SELECT COUNT(*) FROM fct_orders WHERE order_id = 'ORD-999'").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM quarantine_records WHERE quarantine_id = 'q-test-1'").fetchone()[0] == 1
    con.close()

    # Attempt reprocess before customer arrives -> 0 promoted
    count, rev = reprocess_orphan_orders(db_file, run_id="bf_1")
    assert count == 0
    assert rev == 0.0

    # 2. Now the late-arriving customer CUST-888 arrives in dim_customers
    customers = [{
        "customer_id": "CUST-888",
        "first_name": "Late",
        "last_name": "Arriver",
        "full_name": "Late Arriver",
        "email": "late@example.com",
        "phone": "123",
        "address": "123 Way",
        "city": "City",
        "state": "CA",
        "postal_code": "94105",
        "country": "US",
        "registration_date": "2024-03-01T00:00:00Z",
        "updated_at": "2024-03-01T00:00:00Z",
        "is_active": True,
        "source_file": "cust.csv",
        "run_id": "run-2",
        "ingested_at": "2024-03-01T00:00:00Z",
    }]
    publisher.publish_customers(customers)

    # 3. Reprocess quarantine
    count, rev = reprocess_orphan_orders(db_file, run_id="bf_2")
    assert count == 1
    assert rev == 175.50

    # Verify ORD-999 is now in fct_orders and removed from quarantine_records
    con = duckdb.connect(str(db_file), read_only=True)
    assert con.execute("SELECT COUNT(*) FROM fct_orders WHERE order_id = 'ORD-999'").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM quarantine_records WHERE quarantine_id = 'q-test-1'").fetchone()[0] == 0
    con.close()
