"""Shared pytest fixtures, mock configurations, and isolated temporary workspaces."""

import tempfile
from pathlib import Path
import pytest

from config.pipeline_config import PipelineConfig, PathsConfig, ProcessingConfig, ValidationConfig
from src.pipeline.publisher import TargetPublisher


@pytest.fixture
def temp_workspace(tmp_path: Path):
    """Provides an isolated directory tree with all required subdirectories."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    cust_dir = workspace / "data" / "sample"
    cust_dir.mkdir(parents=True)

    curated_dir = workspace / "data" / "curated"
    curated_dir.mkdir(parents=True)

    quarantine_dir = workspace / "data" / "quarantine"
    quarantine_dir.mkdir(parents=True)

    logs_dir = workspace / "logs"
    logs_dir.mkdir(parents=True)

    db_file = curated_dir / "test_warehouse.duckdb"

    return {
        "root": workspace,
        "sample_dir": cust_dir,
        "curated_dir": curated_dir,
        "quarantine_dir": quarantine_dir,
        "logs_dir": logs_dir,
        "db_file": db_file,
    }


@pytest.fixture
def pipeline_config(temp_workspace):
    """Returns a PipelineConfig targeted at the isolated temp workspace."""
    cfg = PipelineConfig(
        name="test_pipeline",
        paths=PathsConfig(
            customers_source=temp_workspace["sample_dir"] / "customers.csv",
            orders_source=temp_workspace["sample_dir"] / "orders.csv",
            database_file=temp_workspace["db_file"],
            curated_dir=temp_workspace["curated_dir"],
            quarantine_dir=temp_workspace["quarantine_dir"],
            logs_dir=temp_workspace["logs_dir"],
        ),
        processing=ProcessingConfig(
            enable_parquet_export=True,
            enable_csv_export=True,
            publish_analytics_marts=True,
        ),
        validation=ValidationConfig(),
    )
    return cfg


@pytest.fixture
def publisher(temp_workspace):
    """Returns an initialized TargetPublisher pointing to the isolated database."""
    return TargetPublisher(
        db_path=temp_workspace["db_file"],
        curated_dir=temp_workspace["curated_dir"],
        quarantine_dir=temp_workspace["quarantine_dir"],
    )
