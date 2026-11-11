"""Publishing layer managing DuckDB warehouse storage, idempotent UPSERTs, and Parquet/CSV file exports."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set
import duckdb
import pandas as pd

from src.pipeline.schemas import QuarantineRecord


class TargetPublisher:
    """Manages publishing of curated data and quarantine rejects to DuckDB and columnar files."""

    def __init__(self, db_path: Path | str, curated_dir: Path | str, quarantine_dir: Path | str):
        self.db_path = str(db_path)
        self.curated_dir = Path(curated_dir)
        self.quarantine_dir = Path(quarantine_dir)
        self.curated_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

        self._init_db()

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _init_db(self) -> None:
        """Initializes tables and indexes in DuckDB."""
        with self._get_connection() as con:
            con.execute("""
            CREATE TABLE IF NOT EXISTS dim_customers (
                customer_id VARCHAR PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                full_name VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                address VARCHAR,
                city VARCHAR,
                state VARCHAR,
                postal_code VARCHAR,
                country VARCHAR,
                registration_date TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE,
                is_active BOOLEAN,
                source_file VARCHAR,
                run_id VARCHAR,
                ingested_at TIMESTAMP WITH TIME ZONE
            );
            """)

            con.execute("""
            CREATE TABLE IF NOT EXISTS fct_orders (
                order_id VARCHAR PRIMARY KEY,
                customer_id VARCHAR,
                order_date TIMESTAMP WITH TIME ZONE,
                order_status VARCHAR,
                total_amount DOUBLE,
                currency VARCHAR,
                payment_method VARCHAR,
                item_count INTEGER,
                average_item_price DOUBLE,
                updated_at TIMESTAMP WITH TIME ZONE,
                source_file VARCHAR,
                run_id VARCHAR,
                ingested_at TIMESTAMP WITH TIME ZONE
            );
            """)

            con.execute("""
            CREATE TABLE IF NOT EXISTS quarantine_records (
                quarantine_id VARCHAR PRIMARY KEY,
                run_id VARCHAR,
                batch_id VARCHAR,
                source_table VARCHAR,
                source_file VARCHAR,
                source_row_number INTEGER,
                record_id VARCHAR,
                failure_code VARCHAR,
                failure_reason VARCHAR,
                raw_payload VARCHAR,
                rejected_at TIMESTAMP WITH TIME ZONE
            );
            """)

            con.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR PRIMARY KEY,
                pipeline_name VARCHAR,
                start_time TIMESTAMP WITH TIME ZONE,
                end_time TIMESTAMP WITH TIME ZONE,
                status VARCHAR,
                customers_read INTEGER,
                customers_curated INTEGER,
                customers_quarantined INTEGER,
                customers_duplicates INTEGER,
                orders_read INTEGER,
                orders_curated INTEGER,
                orders_quarantined INTEGER,
                orders_duplicates INTEGER,
                duration_seconds DOUBLE,
                error_message VARCHAR
            );
            """)

    def get_existing_customer_ids(self) -> Set[str]:
        """Fetches all existing customer_ids in dim_customers for referential integrity checks."""
        with self._get_connection() as con:
            res = con.execute("SELECT customer_id FROM dim_customers").fetchall()
            return {row[0] for row in res}

    def publish_customers(self, customers: List[Dict]) -> int:
        """Idempotently upserts customer records into dim_customers."""
        if not customers:
            return 0

        df = pd.DataFrame(customers)
        with self._get_connection() as con:
            con.register("df_customers_staging", df)
            # Idempotent upsert
            con.execute("""
            INSERT OR REPLACE INTO dim_customers
            SELECT 
                customer_id, first_name, last_name, full_name, email, phone,
                address, city, state, postal_code, country,
                CAST(registration_date AS TIMESTAMP WITH TIME ZONE),
                CAST(updated_at AS TIMESTAMP WITH TIME ZONE),
                is_active, source_file, run_id,
                CAST(ingested_at AS TIMESTAMP WITH TIME ZONE)
            FROM df_customers_staging;
            """)
        return len(customers)

    def publish_orders(self, orders: List[Dict]) -> int:
        """Idempotently upserts order records into fct_orders."""
        if not orders:
            return 0

        df = pd.DataFrame(orders)
        with self._get_connection() as con:
            con.register("df_orders_staging", df)
            # Idempotent upsert
            con.execute("""
            INSERT OR REPLACE INTO fct_orders
            SELECT 
                order_id, customer_id,
                CAST(order_date AS TIMESTAMP WITH TIME ZONE),
                order_status, total_amount, currency, payment_method,
                item_count, average_item_price,
                CAST(updated_at AS TIMESTAMP WITH TIME ZONE),
                source_file, run_id,
                CAST(ingested_at AS TIMESTAMP WITH TIME ZONE)
            FROM df_orders_staging;
            """)
        return len(orders)

    def publish_quarantine(self, quarantined: List[QuarantineRecord]) -> int:
        """Appends quarantined records to quarantine_records table idempotently."""
        if not quarantined:
            return 0

        dict_records = [q.to_dict() for q in quarantined]
        df = pd.DataFrame(dict_records)
        with self._get_connection() as con:
            con.register("df_quarantine_staging", df)
            con.execute("""
            INSERT OR REPLACE INTO quarantine_records
            SELECT 
                quarantine_id, run_id, batch_id, source_table, source_file,
                source_row_number, record_id, failure_code, failure_reason,
                raw_payload,
                CAST(rejected_at AS TIMESTAMP WITH TIME ZONE)
            FROM df_quarantine_staging;
            """)
        return len(quarantined)

    def refresh_analytics_marts(self, run_id: str) -> None:
        """Recomputes aggregate mart agg_customer_metrics from curated tables."""
        with self._get_connection() as con:
            con.execute("""
            CREATE OR REPLACE TABLE agg_customer_metrics AS
            SELECT 
                c.customer_id,
                c.full_name,
                c.email,
                COUNT(o.order_id) AS total_orders,
                COUNT(CASE WHEN o.order_status IN ('DELIVERED', 'SHIPPED') THEN 1 END) AS completed_orders,
                COUNT(CASE WHEN o.order_status = 'CANCELLED' THEN 1 END) AS cancelled_orders,
                COALESCE(ROUND(SUM(o.total_amount), 2), 0.0) AS lifetime_spend,
                COALESCE(ROUND(AVG(o.total_amount), 2), 0.0) AS average_order_value,
                MIN(o.order_date) AS first_order_date,
                MAX(o.order_date) AS latest_order_date,
                CASE 
                    WHEN COUNT(o.order_id) = 0 THEN 'NO_ORDERS'
                    ELSE 'ACTIVE'
                END AS customer_status,
                CURRENT_TIMESTAMP AS last_calculated_at,
                ? AS run_id
            FROM dim_customers c
            LEFT JOIN fct_orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.full_name, c.email;
            """, [run_id])

    def export_files(self, export_parquet: bool = True, export_csv: bool = True) -> Dict[str, str]:
        """Exports tables to Parquet and CSV files in target directories."""
        exported: Dict[str, str] = {}
        with self._get_connection() as con:
            tables = [
                ("dim_customers", self.curated_dir / "dim_customers"),
                ("fct_orders", self.curated_dir / "fct_orders"),
                ("agg_customer_metrics", self.curated_dir / "agg_customer_metrics"),
                ("quarantine_records", self.quarantine_dir / "rejected_records"),
            ]
            for tbl, out_path_base in tables:
                # Check table row count
                cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]

                if export_parquet:
                    pqt_path = out_path_base.with_suffix(".parquet")
                    con.execute(f"COPY {tbl} TO '{str(pqt_path)}' (FORMAT PARQUET);")
                    exported[f"{tbl}_parquet"] = str(pqt_path)

                if export_csv:
                    csv_path = out_path_base.with_suffix(".csv")
                    con.execute(f"COPY {tbl} TO '{str(csv_path)}' (HEADER, DELIMITER ',');")
                    exported[f"{tbl}_csv"] = str(csv_path)

        return exported

    def log_run_metrics(self, metrics: Dict) -> None:
        """Records pipeline execution metrics in pipeline_runs."""
        df = pd.DataFrame([metrics])
        with self._get_connection() as con:
            con.register("df_run_metrics", df)
            con.execute("""
            INSERT OR REPLACE INTO pipeline_runs
            SELECT 
                run_id, pipeline_name,
                CAST(start_time AS TIMESTAMP WITH TIME ZONE),
                CAST(end_time AS TIMESTAMP WITH TIME ZONE),
                status, customers_read, customers_curated, customers_quarantined,
                customers_duplicates, orders_read, orders_curated, orders_quarantined,
                orders_duplicates, duration_seconds, error_message
            FROM df_run_metrics;
            """)
