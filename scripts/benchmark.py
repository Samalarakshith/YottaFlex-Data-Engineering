"""Performance benchmark script comparing ingestion throughput across dataset scales."""

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.pipeline_config import PipelineConfig, ProcessingConfig
from scripts.generate_data import generate_datasets
from src.pipeline.runner import run_pipeline
from tabulate import tabulate


def benchmark_scale(num_customers: int, num_orders: int, bench_dir: Path) -> dict:
    """Runs a complete pipeline cycle for a specific scale and measures throughput."""
    scale_dir = bench_dir / f"scale_{num_orders}"
    scale_dir.mkdir(parents=True, exist_ok=True)

    db_path = scale_dir / "bench.duckdb"
    curated_dir = scale_dir / "curated"
    quarantine_dir = scale_dir / "quarantine"

    print(f"\n---> Generating synthetic dataset: {num_customers:,} customers, {num_orders:,} orders...")
    t0_gen = time.perf_counter()
    cust_csv, ord_csv, _ = generate_datasets(
        num_customers=num_customers,
        num_orders=num_orders,
        seed=42,
        error_rate=0.03,
        output_dir=scale_dir,
        manifest_path=scale_dir / "manifest.json",
    )
    gen_duration = time.perf_counter() - t0_gen

    cfg = PipelineConfig(
        name=f"benchmark_{num_orders}",
        paths=PipelineConfig().paths,
        processing=ProcessingConfig(enable_parquet_export=True, enable_csv_export=True),
    )
    cfg.paths.database_file = db_path
    cfg.paths.curated_dir = curated_dir
    cfg.paths.quarantine_dir = quarantine_dir
    cfg.paths.logs_dir = scale_dir / "logs"

    print(f"---> Running batch pipeline...")
    t0_pipe = time.perf_counter()
    metrics = run_pipeline(
        config=cfg,
        customers_path=cust_csv,
        orders_path=ord_csv,
        run_id=f"bench-{num_orders}",
        batch_id="bench_batch",
    )
    pipe_duration = time.perf_counter() - t0_pipe

    total_records = num_customers + num_orders
    throughput = round(total_records / pipe_duration, 1)

    return {
        "orders_scale": f"{num_orders:,}",
        "customers_scale": f"{num_customers:,}",
        "total_records": f"{total_records:,}",
        "data_gen_sec": f"{gen_duration:.2f} s",
        "pipeline_sec": f"{pipe_duration:.2f} s",
        "throughput_rec_per_sec": f"{throughput:,.1f} rec/s",
        "curated_orders": metrics.orders_curated,
        "quarantined_orders": metrics.orders_quarantined,
    }


def main():
    parser = argparse.ArgumentParser(description="Performance benchmarking suite.")
    parser.add_argument("--quick", action="store_true", help="Run quick benchmark up to 10k orders")
    parser.add_argument("--full", action="store_true", help="Run full benchmark up to 100k orders")
    args = parser.parse_args()

    bench_root = Path("data/benchmark")
    bench_root.mkdir(parents=True, exist_ok=True)

    if args.quick:
        scales = [(100, 1_000), (1_000, 10_000)]
    else:
        # Default runs standard and large scale (100k)
        scales = [(200, 2_000), (2_000, 20_000), (10_000, 100_000)]

    results = []
    for c_scale, o_scale in scales:
        res = benchmark_scale(c_scale, o_scale, bench_root)
        results.append(res)

    print("\n" + "=" * 80)
    print("                     PIPELINE PERFORMANCE BENCHMARK REPORT")
    print("=" * 80)
    headers = [
        "Customers", "Orders", "Total Records", "Gen Time",
        "Pipeline Time", "Throughput", "Curated Orders", "Quarantined Orders"
    ]
    rows = [
        [
            r["customers_scale"], r["orders_scale"], r["total_records"],
            r["data_gen_sec"], r["pipeline_sec"], r["throughput_rec_per_sec"],
            r["curated_orders"], r["quarantined_orders"]
        ]
        for r in results
    ]
    print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    print("=" * 80)


if __name__ == "__main__":
    main()
