# System Design, Architecture Decisions, and Trade-offs

This document outlines the technical design, architectural philosophy, trade-off analysis, idempotency mechanics, performance evaluation, and future roadmap for the YottaFlex Batch Data Engineering Pipeline.

---

## 1. Architectural Philosophy

The objective is to deliver a robust, maintainable, and verifiable data engineering vertical slice capable of ingesting semi-structured CSV data, detecting data-quality anomalies, safely isolating invalid records, and idempotently publishing curated analytical tables.

### Key Tenets
1. **Never Crash on Bad Data**: Corrupt lines, missing fields, or malformed quoting should never abort batch processing; anomalies must be isolated to an explicit quarantine path with complete traceability.
2. **Deterministic & Idempotent**: A pipeline rerun with the same input data must yield the exact same curated state without duplicate rows or financial variance.
3. **Auditable Lineage**: Every published record must record its origin (`source_file`, `run_id`, `ingested_at`).
4. **Reconcilable at All Levels**: Every single input row must be mathematically accounted for:
   $$\text{Source Count} = \text{Curated} + \text{Quarantined} + \text{Superseded Duplicates}$$
5. **Zero External Infrastructure Burden**: Self-contained local execution without requiring complex Docker daemons or cloud accounts, while maintaining cloud-readiness.

---

## 2. Technology Choices & Trade-off Analysis

| Component | Selected Technology | Alternative Considered | Rationale & Trade-offs |
| :--- | :--- | :--- | :--- |
| **Analytical Warehouse** | **DuckDB (Embedded OLAP)** | SQLite, PostgreSQL | **DuckDB** offers vectorized columnar execution, native Parquet export, full SQL-92 support, and zero server management. PostgreSQL requires running a separate service with credentials. SQLite is row-based and slower on large aggregations. |
| **Output File Storage** | **Apache Parquet (PyArrow)** | CSV only, Delta Lake | Parquet provides columnar compression, typed metadata, and high query performance in downstream analytical tools (Presto, Athena, Snowflake, Spark). Delta Lake was considered but adds Spark/JVM dependency. |
| **Configuration** | **YAML + Dataclasses** | Environment variables, `.ini` | Type-safe dataclasses with YAML parsing provide self-documenting hierarchical configuration, IDE autocompletion, and easy CLI overrides. |
| **Data Quality & Validation** | **Python Stream Validator** | Great Expectations, Pandera | A lightweight, pure-Python stream validator was built to parse malformed CSV syntax line-by-line before standard engines drop or crash. Great Expectations introduces excessive boilerplate for batch jobs. |
| **Testing Harness** | **Pytest** | `unittest` | Pytest fixtures allow clean isolation of temporary DuckDB databases, mock directories, and deterministic assertions. |

---

## 3. Detailed Component Architecture

```
                  ┌────────────────────────┐
                  │ Source CSVs            │
                  │ (customers.csv,        │
                  │  orders.csv)           │
                  └───────────┬────────────┘
                              │
                              ▼
                  ┌────────────────────────┐
                  │ Robust CSV Reader      │
                  │ (Line-by-line parser)  │
                  └─────┬────────────┬─────┘
       Malformed syntax │            │ Structured rows
                        ▼            ▼
               ┌──────────────┐   ┌────────────────────────┐
               │              │   │ Schema & Business Rule │
               │              │   │ Validator              │
               │              │   └─────┬────────────┬─────┘
               │              │         │ Violations │ Valid rows
               │              │         ▼            ▼
               │  Quarantine  │   ┌──────────────┐ ┌────────────────────────┐
               │  Records     │   │              │ │ Deduplication Engine   │
               │  (Reject     │   │              │ │ (latest_wins recency)  │
               │   Path)      │   │              │ └─────────┬──────────────┘
               │              │   │              │           │
               │              │   │              │           ▼
               │              │   │              │ ┌────────────────────────┐
               │              │   │              │ │ Referential Integrity  │
               │              │   │              │ │ Verifier               │
               │              │   │              │ └─────────┬──────────────┘
               │              │   │              │  Orphans  │ Validated & Linked
               │              │   │              │◄──────────┤
               │              │   │              │           ▼
               └──────┬───────┘   └──────────────┘ ┌────────────────────────┐
                      │                            │ Transformation &       │
                      │                            │ Enrichment Engine      │
                      │                            └─────────┬──────────────┘
                      │                                      │
                      ▼                                      ▼
               ┌────────────────────────────────────────────────────────────┐
               │                 Idempotent Target Publisher                │
               │               (DuckDB + Parquet Export Engine)             │
               └──────┬──────────────────────┬───────────────────────┬──────┘
                      │                      │                       │
                      ▼                      ▼                       ▼
               ┌──────────────┐      ┌──────────────┐        ┌──────────────┐
               │ dim_customers│      │  fct_orders  │        │ agg_customer │
               │  (Curated)   │      │  (Curated)   │        │   metrics    │
               └──────────────┘      └──────────────┘        └──────────────┘
```

---

## 4. Rejection & Quarantine Path Design

### Why an Explicit Reject Path Matters
In production pipelines, silently dropping invalid records or aborting on the first bad row are anti-patterns. 
- Silently dropping rows leads to silent data loss, financial mismatches, and compliance failures.
- Aborting crashes the entire pipeline, blocking 99.9% of healthy transactions.

### Quarantine Record Attributes
Every rejected row is enriched with:
- `quarantine_id`: Unique UUIDv4.
- `source_table`: Logical table (`customers` or `orders`).
- `source_file`: Verbatim source path.
- `source_row_number`: Line number in the CSV file for exact debugging.
- `failure_code`: Machine-readable categorization (e.g. `INVALID_EMAIL_FORMAT`, `NEGATIVE_OR_ZERO_AMOUNT`).
- `failure_reason`: Descriptive explanation for data stewards.
- `raw_payload`: Unparsed raw row text preserving original state.
- `rejected_at`: UTC timestamp.

Published in both `quarantine_records` (DuckDB) and `data/quarantine/rejected_records.parquet` / `.csv`.

---

## 5. Deduplication & Idempotency Strategy

### 5.1 Deduplication Policies
1. **`latest_wins` (Default)**:
   - Within a batch or across batches, multiple rows with the same primary key (`customer_id` or `order_id`) are ordered by `updated_at`.
   - The row with the latest timestamp is published. Older rows are superseded and tracked in `duplicate_superseded_count`.
2. **`reject_all`**:
   - Any duplicate key is considered an error and routed to quarantine.
3. **`keep_first`**:
   - Keeps the first row encountered in source order.

### 5.2 Idempotency & Reruns
- Curated tables enforce primary key uniqueness via DuckDB's native `INSERT OR REPLACE INTO`.
- Rerunning the exact same input file simply overwrites rows with identical keys, keeping table row counts, distinct IDs, and financial aggregations completely unchanged.
- Operational run logs record each execution with a unique `run_id`, recording start time, duration, row counts, and status (`SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`).

### 5.3 Partial Failure Recovery & Backfill Behavior
- **Partial Failure Recovery**: If a batch has 10 bad rows, the 90 good rows are committed to curated tables, while the 10 bad rows are written to quarantine. When the source system provides corrected data, the batch is re-ingested; the previously missing rows are inserted, and any updated rows are updated in place without duplication.
- **Backfill / Late-Arriving Data**:
  - Orders arriving before their parent customer are quarantined with code `ORPHAN_FOREIGN_KEY`.
  - When the parent customer arrives in a subsequent batch, a backfill query can re-evaluate the quarantine records:
    ```sql
    INSERT OR REPLACE INTO fct_orders
    SELECT ... FROM quarantine_records q
    WHERE q.failure_code = 'ORPHAN_FOREIGN_KEY'
      AND q.record_id IN (SELECT customer_id FROM dim_customers);
    ```

---

## 6. Performance & Benchmark Evaluation

### 6.1 Benchmark Results

Benchmarked on Apple M-series hardware with standard configuration:

| Scale | Customers | Orders | Total Records | Gen Time | Pipeline Time | Throughput | Status |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Small** | 200 | 2,000 | 2,200 | 0.02 s | 0.25 s | **8,702.8 rec/s** | `PARTIAL_SUCCESS` |
| **Medium** | 2,000 | 20,000 | 22,000 | 0.16 s | 1.38 s | **15,936.1 rec/s** | `PARTIAL_SUCCESS` |
| **Large** | 10,000 | 100,000 | 110,000 | 0.79 s | 6.50 s | **16,922.3 rec/s** | `PARTIAL_SUCCESS` |

### 6.2 Performance Reasoning & Reasonable Optimizations

1. **Current Bottleneck**:
   - The current ingestion stage uses Python's standard `csv.reader` in a line-by-line loop to safely capture line numbers and handle malformed rows.
   - Parsing 100,000 rows in Python takes ~4.5 seconds of the total 6.5s runtime.
   - DuckDB SQL upsert and Parquet export take under 1.5 seconds!

2. **Identified Reasonable Optimization**:
   - **Vectorized Chunking / DuckDB Direct Ingestion with Reject Filter**:
     Instead of reading every line in pure Python, use DuckDB's native multi-threaded CSV sniffer:
     `read_csv('orders.csv', ignore_errors=true, all_varchar=true)`.
     Lines rejected by DuckDB can be captured using DuckDB's `rejects_table` feature (introduced in recent DuckDB versions), which processes at **150,000+ records/sec** utilizing all CPU cores.
   - **Chunked Generator Processing**:
     For files exceeding memory (e.g. 10GB+ CSVs), process in streaming chunks of 50,000 rows (`pd.read_csv(chunksize=...)`) yielding memory usage bounded to < 200MB regardless of file size.

---

## 7. Known Limitations & Future Improvements

1. **SCD Type 2 History**: Currently `dim_customers` implements Type 1 (overwrite with latest). In analytical systems tracking historical address changes, an SCD Type 2 pattern (`valid_from`, `valid_to`, `is_current`) would be advantageous.
2. **Schema Drift Handling**: Added columns in source CSVs are ignored gracefully. An automated schema evolution mechanism could dynamically alter target tables if configured.
3. **Automated Alerting**: Quarantine threshold alarms (e.g., alert on Slack/PagerDuty if quarantine rate exceeds 5%) can be triggered directly from `pipeline_runs`.
