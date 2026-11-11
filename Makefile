.PHONY: setup generate-data run test reconcile benchmark clean help analytics backfill ui

PYTHON ?= .venv/bin/python
PIP ?= .venv/bin/pip
PYTEST ?= .venv/bin/pytest

help:
	@echo "YottaFlex Batch Data Engineering Pipeline"
	@echo "Available commands:"
	@echo "  make setup          - Create virtualenv and install dependencies"
	@echo "  make generate-data  - Generate reproducible sample data (100 customers, 500 orders)"
	@echo "  make run            - Execute batch pipeline on sample data"
	@echo "  make test           - Run full test suite with Pytest"
	@echo "  make reconcile      - Run automated reconciliation audit report"
	@echo "  make benchmark      - Run performance benchmark (1k to 100k records)"
	@echo "  make analytics      - Run analytical queries and reporting tables"
	@echo "  make ui             - Launch web dashboard interface at http://127.0.0.1:5050"
	@echo "  make backfill       - Reprocess quarantine records for late-arriving parents"
	@echo "  make clean          - Remove temporary and generated files"

setup:
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

generate-data:
	$(PYTHON) scripts/generate_data.py --customers 100 --orders 500 --seed 42 --error-rate 0.06 --output-dir data/sample

run:
	$(PYTHON) scripts/run_pipeline.py --config config/settings.yaml

test:
	$(PYTEST)

reconcile:
	$(PYTHON) scripts/reconcile.py --db data/curated/warehouse.duckdb

benchmark:
	$(PYTHON) scripts/benchmark.py --quick

benchmark-100k:
	$(PYTHON) scripts/benchmark.py

analytics:
	$(PYTHON) scripts/run_analytics.py

ui:
	$(PYTHON) scripts/run_ui.py

backfill:
	$(PYTHON) scripts/reprocess_quarantine.py

clean:
	rm -rf data/curated/* data/quarantine/* data/benchmark/* logs/* .pytest_cache __pycache__ src/pipeline/__pycache__ tests/__pycache__
