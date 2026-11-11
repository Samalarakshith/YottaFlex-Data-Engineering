"""Reconciliation engine verifying source-to-target row counts, financial totals, and aggregate integrity."""

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
import duckdb
from tabulate import tabulate


@dataclass
class EntityReconciliationResult:
    entity_name: str
    source_count: int
    curated_count: int
    quarantined_count: int
    duplicate_superseded_count: int
    balance_discrepancy: int
    reconciled: bool


@dataclass
class FinancialReconciliationResult:
    fct_orders_total_amount: float
    agg_metrics_total_spend: float
    difference: float
    reconciled: bool


@dataclass
class FullReconciliationReport:
    customers_result: EntityReconciliationResult
    orders_result: EntityReconciliationResult
    financial_result: FinancialReconciliationResult
    all_reconciled: bool

    def print_report(self) -> str:
        out = "\n" + "=" * 70 + "\n"
        out += "           AUTOMATED DATA RECONCILIATION & AUDIT REPORT\n"
        out += "=" * 70 + "\n"

        entity_rows = [
            [
                r.entity_name,
                r.source_count,
                r.curated_count,
                r.quarantined_count,
                r.duplicate_superseded_count,
                f"{r.source_count} == {r.curated_count + r.quarantined_count + r.duplicate_superseded_count}",
                "PASS" if r.reconciled else "FAIL",
            ]
            for r in [self.customers_result, self.orders_result]
        ]

        out += tabulate(
            entity_rows,
            headers=[
                "Entity",
                "Source Read",
                "Curated",
                "Quarantine",
                "Superseded Dups",
                "Balance Equation",
                "Status",
            ],
            tablefmt="fancy_grid",
        )

        out += "\n\nFINANCIAL & AGGREGATE TOTALS RECONCILIATION:\n"
        fin_rows = [
            ["fct_orders SUM(total_amount)", f"${self.financial_result.fct_orders_total_amount:,.2f}"],
            ["agg_customer_metrics SUM(lifetime_spend)", f"${self.financial_result.agg_metrics_total_spend:,.2f}"],
            ["Variance / Discrepancy", f"${self.financial_result.difference:,.2f}"],
            ["Financial Audit Status", "PASS" if self.financial_result.reconciled else "FAIL"],
        ]
        out += tabulate(fin_rows, headers=["Metric", "Value"], tablefmt="fancy_grid")

        out += "\n" + "=" * 70 + "\n"
        overall = "OVERALL STATUS: ALL DATA RECONCILED SUCCESSFULLY (100% AUDIT MATCH)" if self.all_reconciled else "OVERALL STATUS: AUDIT RECONCILIATION FAILED"
        out += f"{overall}\n"
        out += "=" * 70 + "\n"
        return out


def run_reconciliation(
    db_path: Path | str,
    latest_run_id: Optional[str] = None,
) -> FullReconciliationReport:
    """Runs SQL queries against DuckDB to reconcile source counts, curated tables, quarantine, and financials."""
    con = duckdb.connect(str(db_path), read_only=True)

    # 1. Fetch latest run metrics from pipeline_runs
    if latest_run_id:
        run_row = con.execute(
            "SELECT customers_read, customers_curated, customers_quarantined, customers_duplicates, "
            "orders_read, orders_curated, orders_quarantined, orders_duplicates "
            "FROM pipeline_runs WHERE run_id = ?",
            [latest_run_id],
        ).fetchone()
    else:
        run_row = con.execute(
            "SELECT customers_read, customers_curated, customers_quarantined, customers_duplicates, "
            "orders_read, orders_curated, orders_quarantined, orders_duplicates "
            "FROM pipeline_runs ORDER BY start_time DESC LIMIT 1"
        ).fetchone()

    if not run_row:
        raise RuntimeError("No run metrics found in pipeline_runs to reconcile against.")

    (
        c_read, c_curated, c_quar, c_dups,
        o_read, o_curated, o_quar, o_dups,
    ) = run_row

    # Reconcile Customers
    c_balance = c_read - (c_curated + c_quar + c_dups)
    c_reconciled = (c_balance == 0)

    cust_res = EntityReconciliationResult(
        entity_name="customers",
        source_count=c_read,
        curated_count=c_curated,
        quarantined_count=c_quar,
        duplicate_superseded_count=c_dups,
        balance_discrepancy=c_balance,
        reconciled=c_reconciled,
    )

    # Reconcile Orders
    o_balance = o_read - (o_curated + o_quar + o_dups)
    o_reconciled = (o_balance == 0)

    ord_res = EntityReconciliationResult(
        entity_name="orders",
        source_count=o_read,
        curated_count=o_curated,
        quarantined_count=o_quar,
        duplicate_superseded_count=o_dups,
        balance_discrepancy=o_balance,
        reconciled=o_reconciled,
    )

    # Financial Reconciliation
    fct_sum_row = con.execute("SELECT COALESCE(SUM(total_amount), 0.0) FROM fct_orders").fetchone()
    fct_sum = round(float(fct_sum_row[0]), 2)

    agg_sum_row = con.execute("SELECT COALESCE(SUM(lifetime_spend), 0.0) FROM agg_customer_metrics").fetchone()
    agg_sum = round(float(agg_sum_row[0]), 2)

    diff = round(abs(fct_sum - agg_sum), 2)
    fin_reconciled = (diff < 0.01)

    fin_res = FinancialReconciliationResult(
        fct_orders_total_amount=fct_sum,
        agg_metrics_total_spend=agg_sum,
        difference=diff,
        reconciled=fin_reconciled,
    )

    all_pass = c_reconciled and o_reconciled and fin_reconciled

    report = FullReconciliationReport(
        customers_result=cust_res,
        orders_result=ord_res,
        financial_result=fin_res,
        all_reconciled=all_pass,
    )

    # Export report to markdown if curated directory exists
    try:
        report_md_path = Path(db_path).parent / "RECONCILIATION_REPORT.md"
        with open(report_md_path, "w", encoding="utf-8") as f:
            f.write("# Automated Data Reconciliation & Audit Report\n\n")
            f.write("```\n")
            f.write(report.print_report())
            f.write("```\n")
    except Exception:
        pass

    return report
