"""End-to-end integration tests verifying the vertical slice across files, DuckDB, and Parquet exports."""

from pathlib import Path
import duckdb
import pyarrow.parquet as pq

from config.pipeline_config import PipelineConfig, PathsConfig, ProcessingConfig
from src.pipeline.runner import run_pipeline
from src.pipeline.reconciler import run_reconciliation


def test_end_to_end_pipeline_execution(tmp_path: Path):
    # Setup temporary directory tree
    curated_dir = tmp_path / "curated"
    quarantine_dir = tmp_path / "quarantine"
    logs_dir = tmp_path / "logs"
    db_file = curated_dir / "warehouse.duckdb"

    # Minimal customer CSV
    cust_file = tmp_path / "customers.csv"
    cust_file.write_text(
        "customer_id,first_name,last_name,email,phone,address,city,state,postal_code,country,registration_date,updated_at\n"
        "CUST-001,Alice,Smith,alice@example.com,1234567890,101 Market St,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-01T00:00:00Z\n"
        "CUST-002,Bob,Jones,bob@example.com,0987654321,202 Market St,SF,CA,94105,US,2024-01-02T00:00:00Z,2024-01-02T00:00:00Z\n"
    )

    # Minimal orders CSV
    ord_file = tmp_path / "orders.csv"
    ord_file.write_text(
        "order_id,customer_id,order_date,order_status,total_amount,currency,payment_method,item_count,updated_at\n"
        "ORD-001,CUST-001,2024-01-15T10:00:00Z,DELIVERED,120.00,USD,credit_card,2,2024-01-15T10:00:00Z\n"
        "ORD-002,CUST-001,2024-01-20T10:00:00Z,SHIPPED,80.00,USD,apple_pay,1,2024-01-20T10:00:00Z\n"
        "ORD-003,CUST-002,2024-01-25T10:00:00Z,CANCELLED,50.00,USD,paypal,1,2024-01-25T10:00:00Z\n"
    )

    cfg = PipelineConfig(
        name="e2e_test",
        paths=PathsConfig(
            customers_source=cust_file,
            orders_source=ord_file,
            database_file=db_file,
            curated_dir=curated_dir,
            quarantine_dir=quarantine_dir,
            logs_dir=logs_dir,
        ),
        processing=ProcessingConfig(
            enable_parquet_export=True,
            enable_csv_export=True,
            publish_analytics_marts=True,
        ),
    )

    # 1. Run Pipeline
    metrics = run_pipeline(config=cfg, run_id="e2e_run_1")

    assert metrics.status == "SUCCESS"
    assert metrics.customers_curated == 2
    assert metrics.orders_curated == 3
    assert metrics.customers_quarantined == 0
    assert metrics.orders_quarantined == 0

    # 2. Check Database Tables
    con = duckdb.connect(str(db_file), read_only=True)
    c_rows = con.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    o_rows = con.execute("SELECT COUNT(*) FROM fct_orders").fetchone()[0]
    agg_rows = con.execute("SELECT COUNT(*) FROM agg_customer_metrics").fetchone()[0]
    con.close()

    assert c_rows == 2
    assert o_rows == 3
    assert agg_rows == 2

    # 3. Check Parquet Files
    cust_pqt = curated_dir / "dim_customers.parquet"
    ord_pqt = curated_dir / "fct_orders.parquet"
    agg_pqt = curated_dir / "agg_customer_metrics.parquet"

    assert cust_pqt.exists()
    assert ord_pqt.exists()
    assert agg_pqt.exists()

    cust_table = pq.read_table(str(cust_pqt))
    assert cust_table.num_rows == 2

    ord_table = pq.read_table(str(ord_pqt))
    assert ord_table.num_rows == 3

    # 4. Check Customer Aggregates
    con = duckdb.connect(str(db_file), read_only=True)
    c1_metrics = con.execute(
        "SELECT total_orders, completed_orders, cancelled_orders, lifetime_spend, average_order_value "
        "FROM agg_customer_metrics WHERE customer_id = 'CUST-001'"
    ).fetchone()
    con.close()

    assert c1_metrics[0] == 2       # total_orders
    assert c1_metrics[1] == 2       # completed (DELIVERED + SHIPPED)
    assert c1_metrics[2] == 0       # cancelled
    assert c1_metrics[3] == 200.0   # 120 + 80
    assert c1_metrics[4] == 100.0   # avg

    # 5. Run Automated Reconciliation
    recon = run_reconciliation(db_path=db_file, latest_run_id=metrics.run_id)
    assert recon.all_reconciled is True
    assert recon.financial_result.reconciled is True
    assert recon.financial_result.fct_orders_total_amount == 250.0
