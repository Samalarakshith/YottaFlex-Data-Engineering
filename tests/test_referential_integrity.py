"""Unit tests for referential integrity checks between orders and customers."""

from pathlib import Path
from src.pipeline.schemas import FailureCode
from src.pipeline.validator import validate_referential_integrity
from src.pipeline.runner import run_pipeline


def test_orphan_orders_quarantined():
    orders = [
        {"order_id": "ORD-1", "customer_id": "CUST-VALID", "total_amount": 50.0},
        {"order_id": "ORD-2", "customer_id": "CUST-ORPHAN", "total_amount": 75.0},
    ]
    known_customers = {"CUST-VALID"}

    valid_orders, quarantined = validate_referential_integrity(
        orders,
        known_customer_ids=known_customers,
        run_id="r1",
    )

    assert len(valid_orders) == 1
    assert valid_orders[0]["order_id"] == "ORD-1"
    assert len(quarantined) == 1
    assert quarantined[0].failure_code == FailureCode.ORPHAN_FOREIGN_KEY.value
    assert quarantined[0].record_id == "ORD-2"


def test_cross_batch_referential_integrity(pipeline_config, temp_workspace):
    """Verifies that orders in Batch 2 can resolve foreign keys created in Batch 1."""
    # Batch 1: Ingest Customer CUST-999
    c1_path = temp_workspace["sample_dir"] / "cust_b1.csv"
    c1_path.write_text(
        "customer_id,first_name,last_name,email,phone,address,city,state,postal_code,country,registration_date,updated_at\n"
        "CUST-999,Jane,Doe,jane@example.com,1234567890,123 St,City,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"
    )
    empty_ord = temp_workspace["sample_dir"] / "empty_ord.csv"
    empty_ord.write_text("order_id,customer_id,order_date,order_status,total_amount,currency,payment_method,item_count,updated_at\n")

    metrics_b1 = run_pipeline(
        config=pipeline_config,
        customers_path=c1_path,
        orders_path=empty_ord,
        batch_id="batch_1",
    )
    assert metrics_b1.customers_curated == 1

    # Batch 2: Ingest Order referencing CUST-999 with an empty customers file
    empty_cust = temp_workspace["sample_dir"] / "empty_cust.csv"
    empty_cust.write_text("customer_id,first_name,last_name,email,phone,address,city,state,postal_code,country,registration_date,updated_at\n")

    o2_path = temp_workspace["sample_dir"] / "orders_b2.csv"
    o2_path.write_text(
        "order_id,customer_id,order_date,order_status,total_amount,currency,payment_method,item_count,updated_at\n"
        "ORD-555,CUST-999,2024-02-01T00:00:00Z,DELIVERED,150.00,USD,credit_card,2,2024-02-01T00:00:00Z\n"
        "ORD-556,CUST-GHOST,2024-02-01T00:00:00Z,DELIVERED,50.00,USD,credit_card,1,2024-02-01T00:00:00Z\n"
    )

    metrics_b2 = run_pipeline(
        config=pipeline_config,
        customers_path=empty_cust,
        orders_path=o2_path,
        batch_id="batch_2",
    )

    # ORD-555 should succeed (found in warehouse), ORD-556 should be quarantined as orphan
    assert metrics_b2.orders_curated == 1
    assert metrics_b2.orders_quarantined == 1
