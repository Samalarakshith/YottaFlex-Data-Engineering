"""Command-line interface to execute the batch data pipeline."""

import argparse
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.pipeline_config import PipelineConfig
from src.pipeline.runner import run_pipeline


def main():
    parser = argparse.ArgumentParser(description="Execute YottaFlex batch data pipeline.")
    parser.add_argument("--config", type=str, default="config/settings.yaml", help="Path to YAML config")
    parser.add_argument("--customers", type=str, default=None, help="Path to customers CSV (overrides config)")
    parser.add_argument("--orders", type=str, default=None, help="Path to orders CSV (overrides config)")
    parser.add_argument("--batch-id", type=str, default="batch_1", help="Batch identifier for this run")
    parser.add_argument("--run-id", type=str, default=None, help="Explicit run UUID (optional)")

    args = parser.parse_args()

    cfg = PipelineConfig.from_yaml(args.config)
    metrics = run_pipeline(
        config=cfg,
        customers_path=args.customers,
        orders_path=args.orders,
        run_id=args.run_id,
        batch_id=args.batch_id,
    )

    sys.exit(0 if metrics.status in ("SUCCESS", "PARTIAL_SUCCESS") else 1)


if __name__ == "__main__":
    main()
