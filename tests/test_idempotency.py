"""Unit tests for pipeline idempotency, rerun safety, and partial failure recovery."""

import duckdb
from pathlib import Path
from src.pipeline.runner import run_pipeline


def test_rerun_same_input_is_idempotent(pipeline_config, temp_workspace):
    """Verifies that rerunning the exact same input batch causes zero duplicate records."""
    cust_path = temp_workspace["sample_dir"] / "customers.csv"
    cust_path.write_text(
        "customer_id,first_name,last_name,email,phone,address,city,state,postal_code,country,registration_date,updated_at\n"
        "CUST-001,John,Doe,john@example.com,1234567890,123 Main,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"
        "CUST-002,Jane,Doe,jane@example.com,9876543210,456 Side,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"
    )

    ord_path = temp_workspace["sample_dir"] / "orders.csv"
    ord_path.write_text(
        "order_id,customer_id,order_date,order_status,total_amount,currency,payment_method,item_count,updated_at\n"
        "ORD-001,CUST-001,2024-01-10T12:00:00Z,DELIVERED,100.00,USD,credit_card,1,2024-01-10T12:00:00Z\n"
        "ORD-002,CUST-002,2024-01-11T12:00:00Z,DELIVERED,200.00,USD,credit_card,2,2024-01-11T12:00:00Z\n"
    )

    # First Run
    run1 = run_pipeline(config=pipeline_config, run_id="run_101", batch_id="batch_1")
    assert run1.customers_curated == 2
    assert run1.orders_curated == 2

    con = duckdb.connect(str(temp_workspace["db_file"]))
    c_count1 = con.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    o_count1 = con.execute("SELECT COUNT(*) FROM fct_orders").fetchone()[0]
    o_sum1 = con.execute("SELECT SUM(total_amount) FROM fct_orders").fetchone()[0]
    con.close()

    assert c_count1 == 2
    assert o_count1 == 2
    assert o_sum1 == 300.00

    # Second Run with identical inputs
    run2 = run_pipeline(config=pipeline_config, run_id="run_102", batch_id="batch_1")
    assert run2.customers_curated == 2
    assert run2.orders_curated == 2

    con = duckdb.connect(str(temp_workspace["db_file"]))
    c_count2 = con.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    o_count2 = con.execute("SELECT COUNT(*) FROM fct_orders").fetchone()[0]
    o_sum2 = con.execute("SELECT SUM(total_amount) FROM fct_orders").fetchone()[0]
    con.close()

    # Counts and sums MUST remain completely identical
    assert c_count2 == 2
    assert o_count2 == 2
    assert o_sum2 == 300.00


def test_partial_failure_and_retry_recovery(pipeline_config, temp_workspace):
    """Verifies that fixing an erroneous record and retrying gracefully upserts without duplication."""
    cust_path = temp_workspace["sample_dir"] / "customers_partial.csv"
    # Row 1 and 2 valid, Row 3 invalid email
    cust_path.write_text(
        "customer_id,first_name,last_name,email,phone,address,city,state,postal_code,country,registration_date,updated_at\n"
        "CUST-001,John,Doe,john@example.com,1234567890,123 Main,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"
        "CUST-002,Jane,Doe,jane@example.com,9876543210,456 Side,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"
        "CUST-003,Bob,Smith,NOT_A_VALID_EMAIL,5555555555,789 Broad,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"
    )

    empty_ord = temp_workspace["sample_dir"] / "empty_ord.csv"
    empty_ord.write_text("order_id,customer_id,order_date,order_status,total_amount,currency,payment_method,item_count,updated_at\n")

    # Initial Run
    m1 = run_pipeline(
        config=pipeline_config,
        customers_path=cust_path,
        orders_path=empty_ord,
        run_id="run_initial",
    )
    assert m1.customers_curated == 2
    assert m1.customers_quarantined == 1

    # Fix the file: correct the email for CUST-003
    cust_path.write_text(
        "customer_id,first_name,last_name,email,phone,address,city,state,postal_code,country,registration_date,updated_at\n"
        "CUST-001,John,Doe,john@example.com,1234567890,123 Main,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"
        "CUST-002,Jane,Doe,jane@example.com,9876543210,456 Side,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"
        "CUST-003,Bob,Smith,bob@example.com,5555555555,789 Broad,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"
    )

    # Retry Run
    m2 = run_pipeline(
        config=pipeline_config,
        customers_path=cust_path,
        orders_path=empty_ord,
        run_id="run_retry",
    )
    assert m2.customers_curated == 3
    assert m2.customers_quarantined == 0

    con = duckdb.connect(str(temp_workspace["db_file"]))
    c_count = con.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    con.close()

    # Should have exactly 3 total customers, not 5!
    assert c_count == 3
