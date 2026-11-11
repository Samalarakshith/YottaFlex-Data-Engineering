-- ======================================================================
-- YottaFlex Analytical & Audit Queries (DuckDB SQL)
-- Database: data/curated/warehouse.duckdb
-- ======================================================================

-- 1. Top 10 Customers by Lifetime Spend
SELECT 
    customer_id,
    full_name,
    email,
    total_orders,
    completed_orders,
    cancelled_orders,
    ROUND(lifetime_spend, 2) AS lifetime_spend,
    ROUND(average_order_value, 2) AS avg_order_value,
    first_order_date,
    latest_order_date,
    customer_status
FROM agg_customer_metrics
ORDER BY lifetime_spend DESC
LIMIT 10;

-- 2. Daily Sales Trend & Completed Revenue
SELECT 
    CAST(order_date AS DATE) AS order_day,
    COUNT(order_id) AS total_orders,
    COUNT(CASE WHEN order_status IN ('DELIVERED', 'SHIPPED') THEN 1 END) AS successful_orders,
    ROUND(SUM(total_amount), 2) AS gross_revenue,
    ROUND(SUM(CASE WHEN order_status IN ('DELIVERED', 'SHIPPED') THEN total_amount ELSE 0.0 END), 2) AS net_revenue
FROM fct_orders
GROUP BY CAST(order_date AS DATE)
ORDER BY order_day DESC;

-- 3. Quarantine Failure Audit & Breakdown
SELECT 
    source_table,
    failure_code,
    COUNT(*) AS total_failures,
    MIN(source_row_number) AS first_row_number,
    MAX(source_row_number) AS last_row_number
FROM quarantine_records
GROUP BY source_table, failure_code
ORDER BY total_failures DESC;

-- 4. Pipeline Execution History & Data Quality Pass Rate
SELECT 
    run_id,
    start_time,
    status,
    customers_read,
    customers_curated,
    customers_quarantined,
    orders_read,
    orders_curated,
    orders_quarantined,
    ROUND((customers_curated + orders_curated) * 100.0 / NULLIF(customers_read + orders_read, 0), 2) AS overall_pass_rate_pct,
    ROUND(duration_seconds, 3) AS duration_sec
FROM pipeline_runs
ORDER BY start_time DESC;
