#!/usr/bin/env bash
set -e

ACTION="${1:-run}"
PYTHON="${PYTHON:-.venv/bin/python3}"

if [ ! -f "$PYTHON" ]; then
    PYTHON="python3"
fi

case "$ACTION" in
    setup)
        echo "Setting up virtual environment..."
        python3 -m venv .venv
        .venv/bin/pip install -r requirements.txt
        ;;
    generate)
        echo "Generating reproducible sample data..."
        $PYTHON scripts/generate_data.py --customers 100 --orders 500 --seed 42 --error-rate 0.06 --output-dir data/sample
        ;;
    run)
        echo "Executing batch pipeline..."
        $PYTHON scripts/run_pipeline.py --config config/settings.yaml
        ;;
    test)
        echo "Running automated test suite..."
        $PYTHON -m pytest
        ;;
    reconcile)
        echo "Running reconciliation audit..."
        $PYTHON scripts/reconcile.py --db data/curated/warehouse.duckdb
        ;;
    benchmark)
        echo "Running performance benchmark..."
        $PYTHON scripts/benchmark.py --quick
        ;;
    benchmark-100k)
        echo "Running 100k performance benchmark..."
        $PYTHON scripts/benchmark.py
        ;;
    analytics)
        echo "Running analytical queries report..."
        $PYTHON scripts/run_analytics.py
        ;;
    ui)
        echo "Starting YottaFlex Web Interface at http://127.0.0.1:5050..."
        $PYTHON scripts/run_ui.py
        ;;
    backfill)
        echo "Running quarantine backfill / reprocessing..."
        $PYTHON scripts/reprocess_quarantine.py
        ;;
    *)
        echo "Usage: ./run.sh [setup|generate|run|test|reconcile|benchmark|benchmark-100k|analytics|ui|backfill]"
        exit 1
        ;;
esac
