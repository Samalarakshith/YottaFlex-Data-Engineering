from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
import yaml


@dataclass
class ValidationConfig:
    strict_schema: bool = True
    allow_orphan_orders: bool = False
    allowed_order_statuses: List[str] = field(default_factory=lambda: [
        "PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"
    ])
    min_order_amount: float = 0.01
    max_order_amount: float = 1_000_000.00
    min_item_count: int = 1


@dataclass
class ProcessingConfig:
    batch_size: int = 50_000
    deduplication_policy: str = "latest_wins"
    enable_parquet_export: bool = True
    enable_csv_export: bool = True
    publish_analytics_marts: bool = True


@dataclass
class PathsConfig:
    customers_source: Path = Path("data/sample/customers.csv")
    orders_source: Path = Path("data/sample/orders.csv")
    database_file: Path = Path("data/curated/warehouse.duckdb")
    curated_dir: Path = Path("data/curated")
    quarantine_dir: Path = Path("data/quarantine")
    logs_dir: Path = Path("logs")


@dataclass
class PipelineConfig:
    name: str = "ecommerce_batch_ingestion"
    version: str = "1.0.0"
    paths: PathsConfig = field(default_factory=PathsConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)

    @classmethod
    def from_yaml(cls, yaml_path: str | Path) -> "PipelineConfig":
        yaml_file = Path(yaml_path)
        if not yaml_file.exists():
            return cls()

        with open(yaml_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        pipeline_info = data.get("pipeline", {})
        paths_info = data.get("paths", {})
        processing_info = data.get("processing", {})
        validation_info = data.get("validation", {})

        return cls(
            name=pipeline_info.get("name", "ecommerce_batch_ingestion"),
            version=pipeline_info.get("version", "1.0.0"),
            paths=PathsConfig(
                customers_source=Path(paths_info.get("customers_source", "data/sample/customers.csv")),
                orders_source=Path(paths_info.get("orders_source", "data/sample/orders.csv")),
                database_file=Path(paths_info.get("database_file", "data/curated/warehouse.duckdb")),
                curated_dir=Path(paths_info.get("curated_dir", "data/curated")),
                quarantine_dir=Path(paths_info.get("quarantine_dir", "data/quarantine")),
                logs_dir=Path(paths_info.get("logs_dir", "logs")),
            ),
            processing=ProcessingConfig(
                batch_size=processing_info.get("batch_size", 50000),
                deduplication_policy=processing_info.get("deduplication_policy", "latest_wins"),
                enable_parquet_export=processing_info.get("enable_parquet_export", True),
                enable_csv_export=processing_info.get("enable_csv_export", True),
                publish_analytics_marts=processing_info.get("publish_analytics_marts", True),
            ),
            validation=ValidationConfig(
                strict_schema=validation_info.get("strict_schema", True),
                allow_orphan_orders=validation_info.get("allow_orphan_orders", False),
                allowed_order_statuses=validation_info.get("allowed_order_statuses", [
                    "PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"
                ]),
                min_order_amount=float(validation_info.get("min_order_amount", 0.01)),
                max_order_amount=float(validation_info.get("max_order_amount", 1000000.0)),
                min_item_count=int(validation_info.get("min_item_count", 1)),
            ),
        )
