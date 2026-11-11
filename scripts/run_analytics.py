"""Executes analytical reporting queries against the published warehouse."""

import sys
from pathlib import Path
import duckdb
from tabulate import tabulate

DB_PATH = Path("data/curated/warehouse.duckdb")


def main():
    if not DB_PATH.exists():
        print(f"Warehouse database not found at {DB_PATH}. Please run the pipeline first.")
        sys.exit(1)

    con = duckdb.connect(str(DB_PATH), read_only=True)

    print("\n" + "=" * 80)
    print("                      YOTTAFLEX ANALYTICAL REPORTS")
    print("=" * 80)

    # 1. Top 5 Customers by Lifetime Spend
    print("\n[REPORT 1] TOP 5 CUSTOMERS BY LIFETIME SPEND:")
    q1 = """
    SELECT 
        customer_id,
        full_name,
        email,
        total_orders,
        completed_orders,
        cancelled_orders,
        '$' || PRINTF('%.2f', lifetime_spend) AS lifetime_spend,
        '$' || PRINTF('%.2f', average_order_value) AS avg_order_val
    FROM agg_customer_metrics
    ORDER BY lifetime_spend DESC
    LIMIT 5;
    """
    df1 = con.execute(q1).fetchall()
    headers1 = ["Customer ID", "Name", "Email", "Orders", "Completed", "Cancelled", "Spend", "AOV"]
    print(tabulate(df1, headers=headers1, tablefmt="fancy_grid"))

    # 2. Order Distribution by Status
    print("\n[REPORT 2] ORDER DISTRIBUTION BY STATUS:")
    q2 = """
    SELECT 
        order_status,
        COUNT(*) AS order_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) || '%' AS pct_of_total,
        '$' || PRINTF('%.2f', SUM(total_amount)) AS gross_revenue,
        '$' || PRINTF('%.2f', AVG(total_amount)) AS avg_order_amount
    FROM fct_orders
    GROUP BY order_status
    ORDER BY order_count DESC;
    """
    df2 = con.execute(q2).fetchall()
    headers2 = ["Status", "Count", "Percentage", "Gross Revenue", "Avg Amount"]
    print(tabulate(df2, headers=headers2, tablefmt="fancy_grid"))

    # 3. Quarantine Failure Breakdown
    print("\n[REPORT 3] QUARANTINE FAILURE SUMMARY BY SOURCE TABLE:")
    q3 = """
    SELECT 
        source_table,
        failure_code,
        COUNT(*) AS rejected_records,
        MIN(source_row_number) AS first_row_affected,
        MAX(source_row_number) AS last_row_affected
    FROM quarantine_records
    GROUP BY source_table, failure_code
    ORDER BY source_table, rejected_records DESC;
    """
    df3 = con.execute(q3).fetchall()
    headers3 = ["Table", "Failure Code", "Count", "First Row", "Last Row"]
    print(tabulate(df3, headers=headers3, tablefmt="fancy_grid"))

    # 4. Pipeline Execution Audit History
    print("\n[REPORT 4] RECENT PIPELINE RUN AUDIT LOG:")
    q4 = """
    SELECT 
        SUBSTR(run_id, 1, 8) || '...' AS run_id,
        status,
        customers_read,
        customers_curated,
        customers_quarantined,
        orders_read,
        orders_curated,
        orders_quarantined,
        PRINTF('%.3f', duration_seconds) || 's' AS duration
    FROM pipeline_runs
    ORDER BY start_time DESC
    LIMIT 5;
    """
    df4 = con.execute(q4).fetchall()
    headers4 = ["Run ID", "Status", "Cust Read", "Cust Curated", "Cust Quar", "Ord Read", "Ord Curated", "Ord Quar", "Duration"]
    print(tabulate(df4, headers=headers4, tablefmt="fancy_grid"))

    print("\n" + "=" * 80 + "\n")
    con.close()


if __name__ == "__main__":
    main()
