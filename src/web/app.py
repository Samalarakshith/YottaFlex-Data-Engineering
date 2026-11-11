"""Web Interface Server for YottaFlex Data Engineering Pipeline."""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any

from flask import Flask, render_template, jsonify, request
import duckdb

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.pipeline_config import PipelineConfig
from src.pipeline.runner import run_pipeline
from src.pipeline.reconciler import run_reconciliation
from scripts.generate_data import generate_datasets
from scripts.reprocess_quarantine import reprocess_orphan_orders

app = Flask(__name__, template_folder="templates", static_folder="static")

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = BASE_DIR / "data" / "curated" / "warehouse.duckdb"
SAMPLE_DIR = BASE_DIR / "data" / "sample"


def get_db_connection(read_only: bool = True):
    if not DB_PATH.exists():
        # Trigger initial pipeline to populate db if missing
        run_pipeline()
    return duckdb.connect(str(DB_PATH), read_only=read_only)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def get_stats():
    """Returns top-level pipeline metrics, warehouse aggregates, and quarantine breakdowns."""
    if not DB_PATH.exists():
        return jsonify({"status": "uninitialized", "message": "Warehouse not initialized."})

    try:
        con = get_db_connection(read_only=True)

        # 1. Row counts
        cust_count = con.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
        ord_count = con.execute("SELECT COUNT(*) FROM fct_orders").fetchone()[0]
        quar_count = con.execute("SELECT COUNT(*) FROM quarantine_records").fetchone()[0]

        # 2. Financial totals
        fct_rev_row = con.execute("SELECT COALESCE(SUM(total_amount), 0.0) FROM fct_orders").fetchone()
        fct_revenue = round(float(fct_rev_row[0]), 2)

        agg_rev_row = con.execute("SELECT COALESCE(SUM(lifetime_spend), 0.0) FROM agg_customer_metrics").fetchone()
        agg_revenue = round(float(agg_rev_row[0]), 2)

        fin_diff = round(abs(fct_revenue - agg_revenue), 2)

        # 3. Latest run
        latest_run = con.execute("""
            SELECT run_id, pipeline_name, start_time, duration_seconds, status,
                   customers_read, customers_curated, customers_quarantined, customers_duplicates,
                   orders_read, orders_curated, orders_quarantined, orders_duplicates, error_message
            FROM pipeline_runs
            ORDER BY start_time DESC
            LIMIT 1
        """).fetchone()

        latest_run_dict = {}
        if latest_run:
            c_read, c_cur, c_q, c_d = latest_run[5], latest_run[6], latest_run[7], latest_run[8]
            o_read, o_cur, o_q, o_d = latest_run[9], latest_run[10], latest_run[11], latest_run[12]
            total_read = c_read + o_read
            total_curated = c_cur + o_cur
            pass_rate = round((total_curated / total_read * 100), 1) if total_read > 0 else 0.0

            latest_run_dict = {
                "run_id": latest_run[0],
                "pipeline_name": latest_run[1],
                "start_time": str(latest_run[2]),
                "duration_seconds": round(float(latest_run[3]), 3),
                "status": latest_run[4],
                "customers_read": c_read,
                "customers_curated": c_cur,
                "customers_quarantined": c_q,
                "customers_duplicates": c_d,
                "orders_read": o_read,
                "orders_curated": o_cur,
                "orders_quarantined": o_q,
                "orders_duplicates": o_d,
                "pass_rate_pct": pass_rate,
                "error_message": latest_run[13] or "",
            }

        # 4. Quarantine breakdown
        quar_breakdown = con.execute("""
            SELECT failure_code, source_table, COUNT(*) as cnt
            FROM quarantine_records
            GROUP BY failure_code, source_table
            ORDER BY cnt DESC
        """).fetchall()

        breakdown_list = [
            {"failure_code": r[0], "source_table": r[1], "count": r[2]}
            for r in quar_breakdown
        ]

        # 5. Order status breakdown
        status_breakdown = con.execute("""
            SELECT order_status, COUNT(*) as cnt, COALESCE(SUM(total_amount), 0.0) as revenue
            FROM fct_orders
            GROUP BY order_status
            ORDER BY cnt DESC
        """).fetchall()

        orders_status_list = [
            {"status": r[0], "count": r[1], "revenue": round(float(r[2]), 2)}
            for r in status_breakdown
        ]

        # 6. Recent runs (last 5)
        recent_runs = con.execute("""
            SELECT run_id, status, start_time, duration_seconds,
                   customers_curated, orders_curated, (customers_quarantined + orders_quarantined) as total_quar
            FROM pipeline_runs
            ORDER BY start_time DESC
            LIMIT 5
        """).fetchall()

        recent_runs_list = [
            {
                "run_id": r[0],
                "status": r[1],
                "start_time": str(r[2])[:19],
                "duration_seconds": round(float(r[3]), 2),
                "customers_curated": r[4],
                "orders_curated": r[5],
                "total_quarantined": r[6],
            }
            for r in recent_runs
        ]

        con.close()

        return jsonify({
            "status": "success",
            "stats": {
                "customers_count": cust_count,
                "orders_count": ord_count,
                "quarantine_count": quar_count,
                "fct_revenue": fct_revenue,
                "agg_revenue": agg_revenue,
                "revenue_discrepancy": fin_diff,
                "reconciliation_match": fin_diff < 0.01,
            },
            "latest_run": latest_run_dict,
            "quarantine_breakdown": breakdown_list,
            "orders_status": orders_status_list,
            "recent_runs": recent_runs_list,
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/run-pipeline", methods=["POST"])
def trigger_pipeline():
    """Triggers batch pipeline execution with current input files."""
    try:
        cfg = PipelineConfig.from_yaml(BASE_DIR / "config" / "settings.yaml")
        metrics = run_pipeline(config=cfg, batch_id=f"web_batch")
        return jsonify({
            "status": "success",
            "metrics": metrics.to_dict(),
            "summary": metrics.print_summary(),
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/run-reconciliation", methods=["POST"])
def trigger_reconciliation():
    """Runs reconciliation audit queries and returns full report."""
    try:
        report = run_reconciliation(db_path=DB_PATH)
        return jsonify({
            "status": "success",
            "all_reconciled": report.all_reconciled,
            "customers": {
                "source_count": report.customers_result.source_count,
                "curated_count": report.customers_result.curated_count,
                "quarantined_count": report.customers_result.quarantined_count,
                "duplicate_count": report.customers_result.duplicate_superseded_count,
                "reconciled": report.customers_result.reconciled,
            },
            "orders": {
                "source_count": report.orders_result.source_count,
                "curated_count": report.orders_result.curated_count,
                "quarantined_count": report.orders_result.quarantined_count,
                "duplicate_count": report.orders_result.duplicate_superseded_count,
                "reconciled": report.orders_result.reconciled,
            },
            "financial": {
                "fct_orders_total": report.financial_result.fct_orders_total_amount,
                "agg_metrics_total": report.financial_result.agg_metrics_total_spend,
                "difference": report.financial_result.difference,
                "reconciled": report.financial_result.reconciled,
            },
            "report_text": report.print_report(),
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/run-backfill", methods=["POST"])
def trigger_backfill():
    """Runs quarantine backfill to promote resolved orphan orders."""
    try:
        promoted_count, revenue = reprocess_orphan_orders(db_path=DB_PATH, run_id="web_backfill")
        return jsonify({
            "status": "success",
            "promoted_count": promoted_count,
            "promoted_revenue": revenue,
            "message": f"Successfully promoted {promoted_count} orphan orders (${revenue:,.2f}) into curated tables.",
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/run-tests", methods=["POST"])
def trigger_tests():
    """Executes pytest suite and returns stdout and pass/fail summary."""
    try:
        cmd = [sys.executable, "-m", "pytest", "-v", "--tb=line"]
        proc = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=60,
        )

        passed = "25 passed" in proc.stdout or "passed in" in proc.stdout
        return jsonify({
            "status": "success" if proc.returncode == 0 else "failed",
            "passed": proc.returncode == 0,
            "stdout": proc.stdout,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/generate-data", methods=["POST"])
def trigger_generate_data():
    """Generates synthetic dataset with custom parameters."""
    try:
        data = request.json or {}
        num_cust = int(data.get("customers", 100))
        num_ord = int(data.get("orders", 500))
        seed = int(data.get("seed", 42))
        error_rate = float(data.get("error_rate", 0.06))

        cust_p, ord_p, man_p = generate_datasets(
            num_customers=num_cust,
            num_orders=num_ord,
            seed=seed,
            error_rate=error_rate,
            output_dir=SAMPLE_DIR,
            manifest_path=SAMPLE_DIR / "ground_truth_manifest.json",
        )

        # Read generated manifest
        with open(man_p, "r") as f:
            manifest_data = json.load(f)

        return jsonify({
            "status": "success",
            "customers_path": str(cust_p),
            "orders_path": str(ord_p),
            "manifest": manifest_data,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/data/<table_name>")
def browse_table(table_name: str):
    """Browses rows of a table with optional search, filter, and pagination."""
    allowed_tables = {
        "dim_customers",
        "fct_orders",
        "agg_customer_metrics",
        "quarantine_records",
        "pipeline_runs",
    }
    if table_name not in allowed_tables:
        return jsonify({"status": "error", "message": f"Table '{table_name}' not allowed."}), 400

    try:
        limit = min(int(request.args.get("limit", 50)), 200)
        offset = int(request.args.get("offset", 0))
        search = request.args.get("search", "").strip()
        filter_col = request.args.get("filter_col", "").strip()
        filter_val = request.args.get("filter_val", "").strip()

        con = get_db_connection(read_only=True)

        where_clauses = []
        params = []

        if filter_col and filter_val:
            where_clauses.append(f"{filter_col} = ?")
            params.append(filter_val)

        if search:
            # Table-specific search
            if table_name == "dim_customers":
                where_clauses.append("(customer_id ILIKE ? OR full_name ILIKE ? OR email ILIKE ?)")
                params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
            elif table_name == "fct_orders":
                where_clauses.append("(order_id ILIKE ? OR customer_id ILIKE ?)")
                params.extend([f"%{search}%", f"%{search}%"])
            elif table_name == "quarantine_records":
                where_clauses.append("(record_id ILIKE ? OR failure_code ILIKE ? OR raw_payload ILIKE ?)")
                params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        total_cnt = con.execute(f"SELECT COUNT(*) FROM {table_name} {where_sql}", params).fetchone()[0]

        query = f"SELECT * FROM {table_name} {where_sql} LIMIT {limit} OFFSET {offset}"
        df = con.execute(query, params).fetch_df()
        con.close()

        # Format timestamps / floats for clean JSON
        records = df.to_dict(orient="records")
        for r in records:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
                elif v is None:
                    r[k] = ""

        columns = list(df.columns) if not df.empty else []

        return jsonify({
            "status": "success",
            "table": table_name,
            "columns": columns,
            "rows": records,
            "total_count": total_cnt,
            "limit": limit,
            "offset": offset,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


def run_server(port: int = 5050):
    print(f"\n========================================================")
    print(f"   🚀 Starting YottaFlex Data Engineering Web UI")
    print(f"   URL: http://127.0.0.1:{port}")
    print(f"========================================================\n")
    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    run_server(port)
