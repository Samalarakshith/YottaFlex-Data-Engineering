"""Unit tests for the automated reconciliation engine and balance assertions."""

from pathlib import Path
from src.pipeline.reconciler import run_reconciliation
from src.pipeline.runner import run_pipeline


def test_reconciliation_balance_and_financials(pipeline_config, temp_workspace):
    # Customer CSV with 1 valid, 1 malformed, 1 duplicate
    cust_path = temp_workspace["sample_dir"] / "cust_recon.csv"
    cust_path.write_text(
        "customer_id,first_name,last_name,email,phone,address,city,state,postal_code,country,registration_date,updated_at\n"
        "CUST-001,John,Doe,john@example.com,1234567890,123 Main,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-02T00:00:00Z\n"
        "CUST-001,John,Doe,john_new@example.com,1234567890,123 Main,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"  # older duplicate
        "CUST-002,Jane,INVALID_COLUMNS,extra1,extra2\n"  # malformed
    )

    # Orders CSV with 1 valid, 1 orphan, 1 negative amount
    ord_path = temp_workspace["sample_dir"] / "ord_recon.csv"
    ord_path.write_text(
        "order_id,customer_id,order_date,order_status,total_amount,currency,payment_method,item_count,updated_at\n"
        "ORD-001,CUST-001,2024-01-10T12:00:00Z,DELIVERED,150.50,USD,credit_card,1,2024-01-10T12:00:00Z\n"
        "ORD-002,CUST-GHOST,2024-01-10T12:00:00Z,DELIVERED,200.00,USD,credit_card,1,2024-01-10T12:00:00Z\n"  # orphan
        "ORD-003,CUST-001,2024-01-10T12:00:00Z,DELIVERED,-50.00,USD,credit_card,1,2024-01-10T12:00:00Z\n"   # negative amount
    )

    run_metrics = run_pipeline(
        config=pipeline_config,
        customers_path=cust_path,
        orders_path=ord_path,
        run_id="run_recon_test",
    )

    report = run_reconciliation(db_path=temp_workspace["db_file"], latest_run_id=run_metrics.run_id)

    # Assertions
    assert report.all_reconciled is True
    assert report.customers_result.reconciled is True
    assert report.orders_result.reconciled is True
    assert report.financial_result.reconciled is True

    # Check exact balance math
    # Customers: 3 read == 1 curated + 1 quarantined + 1 duplicate superseded
    assert report.customers_result.source_count == 3
    assert report.customers_result.curated_count == 1
    assert report.customers_result.quarantined_count == 1
    assert report.customers_result.duplicate_superseded_count == 1

    # Orders: 3 read == 1 curated + 2 quarantined + 0 duplicate
    assert report.orders_result.source_count == 3
    assert report.orders_result.curated_count == 1
    assert report.orders_result.quarantined_count == 2
    assert report.orders_result.duplicate_superseded_count == 0

    # Financial: exactly $150.50 in fct_orders and agg_customer_metrics
    assert report.financial_result.fct_orders_total_amount == 150.50
    assert report.financial_result.agg_metrics_total_spend == 150.50
    assert report.financial_result.difference == 0.0
