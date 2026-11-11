"""Batch Pipeline Orchestrator coordinating ingestion, validation, deduplication, transformation, and publishing."""

import sys
from pathlib import Path
from typing import Optional

from config.pipeline_config import PipelineConfig
from src.pipeline.metrics import PipelineRunMetrics, setup_logger
from src.pipeline.publisher import TargetPublisher
from src.pipeline.reader import read_csv_safely
from src.pipeline.schemas import (
    CUSTOMER_REQUIRED_COLUMNS,
    ORDER_REQUIRED_COLUMNS,
)
from src.pipeline.transformer import (
    deduplicate_records,
    transform_customers,
    transform_orders,
)
from src.pipeline.validator import (
    validate_customers,
    validate_orders,
    validate_referential_integrity,
)


def run_pipeline(
    config: Optional[PipelineConfig] = None,
    customers_path: Optional[Path | str] = None,
    orders_path: Optional[Path | str] = None,
    run_id: Optional[str] = None,
    batch_id: str = "batch_1",
) -> PipelineRunMetrics:
    """Executes the end-to-end batch ingestion and curated publishing pipeline.

    Args:
        config: Pipeline configuration object (defaults to settings.yaml).
        customers_path: Optional override for customers CSV path.
        orders_path: Optional override for orders CSV path.
        run_id: Optional explicit run identifier.
        batch_id: Identifier for current batch.

    Returns:
        PipelineRunMetrics containing row counts, timing, and operational status.
    """
    if config is None:
        config = PipelineConfig.from_yaml("config/settings.yaml")

    cust_file = Path(customers_path or config.paths.customers_source)
    ord_file = Path(orders_path or config.paths.orders_source)

    metrics = PipelineRunMetrics(pipeline_name=config.name)
    if run_id:
        metrics.run_id = run_id

    logger = setup_logger(config.paths.logs_dir, metrics.run_id)
    logger.info(f"Starting batch pipeline run {metrics.run_id} (batch={batch_id})")
    logger.info(f"Customers source: {cust_file}")
    logger.info(f"Orders source: {ord_file}")

    publisher = TargetPublisher(
        db_path=config.paths.database_file,
        curated_dir=config.paths.curated_dir,
        quarantine_dir=config.paths.quarantine_dir,
    )

    all_quarantined = []

    try:
        # ==========================================
        # 1. Ingest & Process Customers
        # ==========================================
        logger.info("Ingesting customers CSV...")
        cust_ingest = read_csv_safely(
            file_path=cust_file,
            table_name="customers",
            required_columns=CUSTOMER_REQUIRED_COLUMNS,
            run_id=metrics.run_id,
            batch_id=batch_id,
            strict_schema=config.validation.strict_schema,
        )
        metrics.customers_read = cust_ingest.total_rows_read
        all_quarantined.extend(cust_ingest.quarantined)

        # Validate customer business rules
        valid_customers_raw, cust_quarantined = validate_customers(
            cust_ingest.records,
            run_id=metrics.run_id,
            batch_id=batch_id,
        )
        all_quarantined.extend(cust_quarantined)

        # Deduplicate customers
        deduped_customers, cust_dup_quarantined, cust_dup_count = deduplicate_records(
            valid_customers_raw,
            key_field="customer_id",
            timestamp_field="updated_at",
            policy=config.processing.deduplication_policy,
            run_id=metrics.run_id,
            batch_id=batch_id,
            table_name="customers",
        )
        all_quarantined.extend(cust_dup_quarantined)
        metrics.customers_duplicates = cust_dup_count

        # Transform customers
        curated_customers = transform_customers(deduped_customers, run_id=metrics.run_id)

        # Publish customers to warehouse
        loaded_cust_count = publisher.publish_customers(curated_customers)
        metrics.customers_curated = loaded_cust_count
        logger.info(f"Customers published: {loaded_cust_count} curated, {cust_dup_count} duplicate superseded.")

        # ==========================================
        # 2. Ingest & Process Orders
        # ==========================================
        logger.info("Ingesting orders CSV...")
        ord_ingest = read_csv_safely(
            file_path=ord_file,
            table_name="orders",
            required_columns=ORDER_REQUIRED_COLUMNS,
            run_id=metrics.run_id,
            batch_id=batch_id,
            strict_schema=config.validation.strict_schema,
        )
        metrics.orders_read = ord_ingest.total_rows_read
        all_quarantined.extend(ord_ingest.quarantined)

        # Validate order business rules & types
        valid_orders_raw, ord_quarantined = validate_orders(
            ord_ingest.records,
            run_id=metrics.run_id,
            batch_id=batch_id,
            min_amount=config.validation.min_order_amount,
            max_amount=config.validation.max_order_amount,
            min_items=config.validation.min_item_count,
        )
        all_quarantined.extend(ord_quarantined)

        # Referential Integrity: check order customer_ids
        known_customers = publisher.get_existing_customer_ids()
        current_valid_cust_ids = {c["customer_id"] for c in curated_customers}
        all_known_customers = known_customers.union(current_valid_cust_ids)

        if not config.validation.allow_orphan_orders:
            retained_orders, ri_quarantined = validate_referential_integrity(
                valid_orders_raw,
                known_customer_ids=all_known_customers,
                run_id=metrics.run_id,
                batch_id=batch_id,
            )
            all_quarantined.extend(ri_quarantined)
        else:
            retained_orders = valid_orders_raw

        # Deduplicate orders
        deduped_orders, ord_dup_quarantined, ord_dup_count = deduplicate_records(
            retained_orders,
            key_field="order_id",
            timestamp_field="updated_at",
            policy=config.processing.deduplication_policy,
            run_id=metrics.run_id,
            batch_id=batch_id,
            table_name="orders",
        )
        all_quarantined.extend(ord_dup_quarantined)
        metrics.orders_duplicates = ord_dup_count

        # Transform orders
        curated_orders = transform_orders(deduped_orders, run_id=metrics.run_id)

        # Publish orders to warehouse
        loaded_ord_count = publisher.publish_orders(curated_orders)
        metrics.orders_curated = loaded_ord_count
        logger.info(f"Orders published: {loaded_ord_count} curated, {ord_dup_count} duplicate superseded.")

        # ==========================================
        # 3. Publish Quarantined Rejects
        # ==========================================
        metrics.customers_quarantined = sum(1 for q in all_quarantined if q.source_table == "customers")
        metrics.orders_quarantined = sum(1 for q in all_quarantined if q.source_table == "orders")
        logger.info(f"Publishing {len(all_quarantined)} quarantined records...")
        publisher.publish_quarantine(all_quarantined)

        # ==========================================
        # 4. Refresh Analytics Marts
        # ==========================================
        if config.processing.publish_analytics_marts:
            logger.info("Refreshing agg_customer_metrics analytical mart...")
            publisher.refresh_analytics_marts(run_id=metrics.run_id)

        # ==========================================
        # 5. File Exports (Parquet & CSV)
        # ==========================================
        exported = publisher.export_files(
            export_parquet=config.processing.enable_parquet_export,
            export_csv=config.processing.enable_csv_export,
        )
        logger.info(f"Exported files: {list(exported.keys())}")

        # Finish metrics successfully
        status = "SUCCESS" if (metrics.customers_quarantined == 0 and metrics.orders_quarantined == 0) else "PARTIAL_SUCCESS"
        metrics.record_quarantine_breakdown(all_quarantined)
        metrics.finish(status=status)
        publisher.log_run_metrics(metrics.to_dict())

        logger.info(f"Pipeline run completed with status={status} in {metrics.duration_seconds:.3f}s")
        print(metrics.print_summary())

        return metrics

    except Exception as e:
        logger.exception(f"Pipeline execution encountered unhandled error: {str(e)}")
        metrics.finish(status="FAILED", error_msg=str(e))
        publisher.log_run_metrics(metrics.to_dict())
        print(metrics.print_summary())
        raise
