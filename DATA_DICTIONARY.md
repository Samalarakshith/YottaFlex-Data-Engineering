# Data Dictionary & Schema Specification

This document defines the source data models, target analytical schemas, operational audit tables, and quarantine reject structures for the YottaFlex Batch Data Pipeline.

---

## 1. Nomenclature & Design Conventions

- **Naming Conventions**: All database tables, Parquet files, and column names use lowercase `snake_case`.
- **Timestamp Standard**: All timestamps follow ISO-8601 (`YYYY-MM-DDTHH:MM:SSZ` or UTC offset) stored as `TIMESTAMP WITH TIME ZONE`.
- **Deterministic Identifiers**: Primary keys are alphanumeric identifiers (`CUST-XXXXX`, `ORD-XXXXXX`) or UUIDv4 for run and quarantine records.
- **Audit Lineage**: Every curated record is stamped with `run_id`, `source_file`, and `ingested_at`.
- **Grain Definition**:
  - `customers`: 1 row per unique customer entity.
  - `orders`: 1 row per individual customer order transaction.

---

## 2. Source Schemas

### 2.1 `customers` (Source CSV)
Input file: `data/sample/customers.csv`

| Column Name | Data Type | Nullable | Primary Key | Description & Validation Rule |
| :--- | :--- | :---: | :---: | :--- |
| `customer_id` | String | No | Yes | Unique customer identifier (e.g., `CUST-00001`). Must not be null or blank. |
| `first_name` | String | No | No | Customer's first name. Required. |
| `last_name` | String | No | No | Customer's last name. Required. |
| `email` | String | No | No | Email address. Validated against standard RFC email regex. |
| `phone` | String | Yes | No | Contact phone number. |
| `address` | String | Yes | No | Street address. |
| `city` | String | Yes | No | City of residence. |
| `state` | String | Yes | No | State or province (normalized to uppercase). |
| `postal_code` | String | Yes | No | Postal or ZIP code. |
| `country` | String | No | No | Country code (defaults to 'US', normalized to uppercase). |
| `registration_date` | Timestamp | No | No | Account creation timestamp (ISO-8601). |
| `updated_at` | Timestamp | No | No | Last modification timestamp (used for recency deduplication). |

---

### 2.2 `orders` (Source CSV)
Input file: `data/sample/orders.csv`

| Column Name | Data Type | Nullable | Primary Key | Foreign Key | Description & Validation Rule |
| :--- | :--- | :---: | :---: | :---: | :--- |
| `order_id` | String | No | Yes | No | Unique order transaction identifier (e.g., `ORD-000001`). |
| `customer_id` | String | No | No | Yes | References `customers.customer_id`. Checked for referential integrity. |
| `order_date` | Timestamp | No | No | No | Transaction placement timestamp (ISO-8601). |
| `order_status` | String | No | No | No | Order state. Must be one of: `PENDING`, `PROCESSING`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `RETURNED`. |
| `total_amount` | Float | No | No | No | Gross order total in currency units. Must be between `$0.01` and `$1,000,000.00`. |
| `currency` | String | No | No | No | 3-letter currency code (e.g. `USD`). Normalized to uppercase. |
| `payment_method` | String | No | No | No | Payment method used (e.g. `credit_card`, `paypal`, `apple_pay`). |
| `item_count` | Integer | No | No | No | Count of line items in order. Must be integer `>= 1`. |
| `updated_at` | Timestamp | No | No | No | Last order update timestamp (used for recency deduplication). |

---

## 3. Curated Target Schemas

### 3.1 `dim_customers` (Curated Customer Dimension)
Storage: DuckDB table & `data/curated/dim_customers.parquet`

| Column Name | Target SQL Type | Description |
| :--- | :--- | :--- |
| `customer_id` | `VARCHAR PRIMARY KEY` | Deterministic unique customer ID. |
| `first_name` | `VARCHAR` | Cleaned first name. |
| `last_name` | `VARCHAR` | Cleaned last name. |
| `full_name` | `VARCHAR` | Derived: `first_name || ' ' || last_name`. |
| `email` | `VARCHAR` | Normalized lowercase email address. |
| `phone` | `VARCHAR` | Cleaned phone string. |
| `address` | `VARCHAR` | Street address. |
| `city` | `VARCHAR` | City name. |
| `state` | `VARCHAR` | Uppercase state code. |
| `postal_code` | `VARCHAR` | Postal code. |
| `country` | `VARCHAR` | Country code. |
| `registration_date` | `TIMESTAMP WITH TIME ZONE` | Account creation timestamp. |
| `updated_at` | `TIMESTAMP WITH TIME ZONE` | Latest modification timestamp. |
| `is_active` | `BOOLEAN` | Account status flag (default `true`). |
| `source_file` | `VARCHAR` | Source CSV file path for data lineage. |
| `run_id` | `VARCHAR` | Pipeline run UUID that published this version. |
| `ingested_at` | `TIMESTAMP WITH TIME ZONE` | Pipeline execution timestamp. |

---

### 3.2 `fct_orders` (Curated Order Fact Table)
Storage: DuckDB table & `data/curated/fct_orders.parquet`

| Column Name | Target SQL Type | Description |
| :--- | :--- | :--- |
| `order_id` | `VARCHAR PRIMARY KEY` | Deterministic unique order transaction ID. |
| `customer_id` | `VARCHAR` | Referential foreign key to `dim_customers(customer_id)`. |
| `order_date` | `TIMESTAMP WITH TIME ZONE` | Order transaction timestamp. |
| `order_status` | `VARCHAR` | Validated order status (`DELIVERED`, `SHIPPED`, etc.). |
| `total_amount` | `DOUBLE` | Validated gross order amount. |
| `currency` | `VARCHAR` | Standardized uppercase currency code (`USD`). |
| `payment_method` | `VARCHAR` | Normalized payment method string. |
| `item_count` | `INTEGER` | Validated line item count (`>= 1`). |
| `average_item_price` | `DOUBLE` | Derived attribute: `ROUND(total_amount / item_count, 2)`. |
| `updated_at` | `TIMESTAMP WITH TIME ZONE` | Source system modification timestamp. |
| `source_file` | `VARCHAR` | Source CSV file path for lineage. |
| `run_id` | `VARCHAR` | Pipeline run UUID. |
| `ingested_at` | `TIMESTAMP WITH TIME ZONE` | Ingestion timestamp. |

---

### 3.3 `agg_customer_metrics` (Curated Analytical Summary Mart)
Storage: DuckDB table & `data/curated/agg_customer_metrics.parquet`

| Column Name | Target SQL Type | Description |
| :--- | :--- | :--- |
| `customer_id` | `VARCHAR PRIMARY KEY` | Customer identifier. |
| `full_name` | `VARCHAR` | Customer full name. |
| `email` | `VARCHAR` | Customer email. |
| `total_orders` | `BIGINT` | Total orders placed by customer. |
| `completed_orders` | `BIGINT` | Orders in `DELIVERED` or `SHIPPED` status. |
| `cancelled_orders` | `BIGINT` | Orders in `CANCELLED` status. |
| `lifetime_spend` | `DOUBLE` | Sum of `total_amount` across customer's orders. |
| `average_order_value` | `DOUBLE` | Average spend per order (`lifetime_spend / total_orders`). |
| `first_order_date` | `TIMESTAMP WITH TIME ZONE` | Timestamp of first recorded order. |
| `latest_order_date` | `TIMESTAMP WITH TIME ZONE` | Timestamp of most recent order. |
| `customer_status` | `VARCHAR` | Customer segment: `ACTIVE` or `NO_ORDERS`. |
| `last_calculated_at` | `TIMESTAMP WITH TIME ZONE` | Mart refresh timestamp. |
| `run_id` | `VARCHAR` | Pipeline run UUID that computed the aggregate. |

---

## 4. Operational & Audit Schemas

### 4.1 `quarantine_records` (Reject Path Table)
Storage: DuckDB table, `data/quarantine/rejected_records.parquet`, and CSV

| Column Name | SQL Type | Description |
| :--- | :--- | :--- |
| `quarantine_id` | `VARCHAR PRIMARY KEY` | UUIDv4 uniquely identifying the rejection event. |
| `run_id` | `VARCHAR` | Pipeline run during which rejection occurred. |
| `batch_id` | `VARCHAR` | Batch identifier. |
| `source_table` | `VARCHAR` | Entity name (`customers` or `orders`). |
| `source_file` | `VARCHAR` | Source file path where record originated. |
| `source_row_number` | `INTEGER` | Exact row line number in the source CSV file. |
| `record_id` | `VARCHAR` | Extracted identifier (`customer_id` or `order_id`) if available. |
| `failure_code` | `VARCHAR` | Standardized machine-readable error code. |
| `failure_reason` | `VARCHAR` | Detailed human-readable explanation of rejection. |
| `raw_payload` | `VARCHAR` | Verbatim raw CSV line or record payload (preserves source state). |
| `rejected_at` | `TIMESTAMP WITH TIME ZONE` | Timestamp of quarantine event. |

#### Standardized Failure Codes
- `MALFORMED_CSV_ROW`: Syntax error, unescaped quote, or column count mismatch.
- `MISSING_REQUIRED_FIELD`: Non-nullable field is missing or empty.
- `NULL_PRIMARY_KEY`: Primary key is null or blank.
- `INVALID_EMAIL_FORMAT`: Customer email fails RFC regex format.
- `INVALID_TIMESTAMP`: Date string fails ISO-8601 parsing.
- `INVALID_NUMERIC`: Value cannot be converted to numeric float/int.
- `NEGATIVE_OR_ZERO_AMOUNT`: `total_amount` is `<= 0.00` or below minimum threshold.
- `EXCEEDS_MAX_AMOUNT`: `total_amount` exceeds configured maximum ($1,000,000).
- `INVALID_ITEM_COUNT`: `item_count` is `< 1` or non-integer.
- `INVALID_ORDER_STATUS`: Status is not in the enumerated valid domain list.
- `ORPHAN_FOREIGN_KEY`: Order references a `customer_id` not found in `dim_customers`.
- `UNEXPECTED_SCHEMA`: Source CSV header lacks required columns.
- `DUPLICATE_KEY_QUARANTINED`: Duplicate key rejected under `reject_all` policy.

---

### 4.2 `pipeline_runs` (Operational Run Log)
Storage: DuckDB table `pipeline_runs`

| Column Name | SQL Type | Description |
| :--- | :--- | :--- |
| `run_id` | `VARCHAR PRIMARY KEY` | Unique execution UUID. |
| `pipeline_name` | `VARCHAR` | Logical pipeline name. |
| `start_time` | `TIMESTAMP WITH TIME ZONE` | Execution start timestamp. |
| `end_time` | `TIMESTAMP WITH TIME ZONE` | Execution end timestamp. |
| `status` | `VARCHAR` | `SUCCESS`, `PARTIAL_SUCCESS`, or `FAILED`. |
| `customers_read` | `INTEGER` | Total raw customer lines ingested. |
| `customers_curated` | `INTEGER` | Valid customers published to `dim_customers`. |
| `customers_quarantined` | `INTEGER` | Customer rows sent to quarantine. |
| `customers_duplicates` | `INTEGER` | Customer duplicate records superseded. |
| `orders_read` | `INTEGER` | Total raw order lines ingested. |
| `orders_curated` | `INTEGER` | Valid orders published to `fct_orders`. |
| `orders_quarantined` | `INTEGER` | Order rows sent to quarantine. |
| `orders_duplicates` | `INTEGER` | Order duplicate records superseded. |
| `duration_seconds` | `DOUBLE` | Total wall-clock runtime in seconds. |
| `error_message` | `VARCHAR` | Fatal error message if status is `FAILED`. |
