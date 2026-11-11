"""Operational metrics collector and structured logging for batch pipeline runs."""

import logging
import os
import sys
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from tabulate import tabulate

from src.pipeline.schemas import QuarantineRecord


def setup_logger(log_dir: Path | str, run_id: str) -> logging.Logger:
    """Configures structured file and console logger for the pipeline run."""
    dir_path = Path(log_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    log_file = dir_path / f"pipeline_run_{run_id[:8]}.log"

    logger = logging.getLogger(f"pipeline_{run_id}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    # Formatter
    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File Handler
    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


@dataclass
class PipelineRunMetrics:
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str = "ecommerce_batch_ingestion"
    start_time: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    end_time: Optional[str] = None
    status: str = "IN_PROGRESS"

    # Customers counts
    customers_read: int = 0
    customers_curated: int = 0
    customers_quarantined: int = 0
    customers_duplicates: int = 0

    # Orders counts
    orders_read: int = 0
    orders_curated: int = 0
    orders_quarantined: int = 0
    orders_duplicates: int = 0

    # Duration and errors
    duration_seconds: float = 0.0
    error_message: str = ""

    # Failure code counters
    quarantine_breakdown: Dict[str, int] = field(default_factory=dict)

    def finish(self, status: str = "SUCCESS", error_msg: str = "") -> None:
        self.status = status
        self.error_message = error_msg
        self.end_time = datetime.now(timezone.utc).isoformat()
        start_dt = datetime.fromisoformat(self.start_time)
        end_dt = datetime.fromisoformat(self.end_time)
        self.duration_seconds = round((end_dt - start_dt).total_seconds(), 4)

    def record_quarantine_breakdown(self, records: List[QuarantineRecord]) -> None:
        counts = Counter(r.failure_code for r in records)
        self.quarantine_breakdown = dict(counts)

    def to_dict(self) -> Dict:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "start_time": self.start_time,
            "end_time": self.end_time or datetime.now(timezone.utc).isoformat(),
            "status": self.status,
            "customers_read": self.customers_read,
            "customers_curated": self.customers_curated,
            "customers_quarantined": self.customers_quarantined,
            "customers_duplicates": self.customers_duplicates,
            "orders_read": self.orders_read,
            "orders_curated": self.orders_curated,
            "orders_quarantined": self.orders_quarantined,
            "orders_duplicates": self.orders_duplicates,
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message,
        }

    def print_summary(self) -> str:
        summary_table = [
            ["Run ID", self.run_id],
            ["Pipeline", self.pipeline_name],
            ["Status", self.status],
            ["Duration (sec)", f"{self.duration_seconds:.3f} s"],
            ["Customers Ingested", self.customers_read],
            ["Customers Curated", self.customers_curated],
            ["Customers Quarantined", self.customers_quarantined],
            ["Customers Duplicates", self.customers_duplicates],
            ["Orders Ingested", self.orders_read],
            ["Orders Curated", self.orders_curated],
            ["Orders Quarantined", self.orders_quarantined],
            ["Orders Duplicates", self.orders_duplicates],
        ]
        if self.error_message:
            summary_table.append(["Error", self.error_message])

        out = "\n" + "=" * 60 + "\n"
        out += "          OPERATIONAL PIPELINE METRICS SUMMARY\n"
        out += "=" * 60 + "\n"
        out += tabulate(summary_table, headers=["Metric", "Value"], tablefmt="fancy_grid")

        if self.quarantine_breakdown:
            out += "\n\nQUARANTINE FAILURE BREAKDOWN:\n"
            q_table = [[k, v] for k, v in sorted(self.quarantine_breakdown.items())]
            out += tabulate(q_table, headers=["Failure Code", "Count"], tablefmt="fancy_grid")
        out += "\n" + "=" * 60 + "\n"
        return out
