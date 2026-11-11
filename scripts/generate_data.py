"""Reproducible and scalable synthetic dataset generator for Customers and Orders with controlled anomaly injection."""

import argparse
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple


FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Evan", "Fiona", "George", "Hannah",
    "Ian", "Julia", "Kevin", "Laura", "Michael", "Nina", "Oliver", "Paula",
    "Quinn", "Rachel", "Samuel", "Tara", "Umar", "Victoria", "William", "Xena", "Yusuf", "Zoe"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"
]

DOMAINS = ["example.com", "yottaflex.io", "techcorp.org", "store.net", "dataflow.ai"]
STATES = ["CA", "NY", "TX", "WA", "IL", "FL", "MA", "CO", "NC", "GA"]
PAYMENT_METHODS = ["credit_card", "paypal", "apple_pay", "bank_transfer"]
ORDER_STATUSES = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]


def generate_datasets(
    num_customers: int,
    num_orders: int,
    seed: int = 42,
    error_rate: float = 0.05,
    output_dir: Path | str = "data/sample",
    manifest_path: Path | str = "data/sample/ground_truth_manifest.json",
) -> Tuple[Path, Path, Path]:
    """Generates synthetic CSV files for customers and orders with reproducible anomalies.

    Args:
        num_customers: Number of customer rows to generate.
        num_orders: Number of order rows to generate.
        seed: Random seed for deterministic generation.
        error_rate: Probability of injecting an anomaly into a row.
        output_dir: Output folder.
        manifest_path: JSON path for expected validation ground truth.

    Returns:
        Tuple of (customers_csv_path, orders_csv_path, manifest_json_path).
    """
    random.seed(seed)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    # Manifest tracking
    manifest = {
        "metadata": {
            "seed": seed,
            "error_rate": error_rate,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
        "customers": {
            "total_rows": num_customers,
            "expected_valid": 0,
            "expected_quarantined": 0,
            "expected_duplicates": 0,
            "breakdown": {},
        },
        "orders": {
            "total_rows": num_orders,
            "expected_valid": 0,
            "expected_quarantined": 0,
            "expected_duplicates": 0,
            "expected_revenue_sum": 0.0,
            "breakdown": {},
        },
    }

    # 1. Generate Customers
    cust_rows: List[List[str]] = []
    cust_header = [
        "customer_id", "first_name", "last_name", "email", "phone",
        "address", "city", "state", "postal_code", "country",
        "registration_date", "updated_at"
    ]

    valid_customer_ids: List[str] = []
    all_generated_cust_ids: List[str] = []

    for i in range(1, num_customers + 1):
        cust_id = f"CUST-{i:05d}"
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        email = f"{first.lower()}.{last.lower()}_{i}@{random.choice(DOMAINS)}"
        phone = f"+1-{random.randint(200,999)}-555-{random.randint(1000,9999)}"
        address = f"{random.randint(100, 9999)} Market St"
        city = "San Francisco"
        state = random.choice(STATES)
        postal = f"{random.randint(10000, 99999)}"
        country = "US"
        reg_dt = (base_time + timedelta(days=random.randint(0, 180), hours=random.randint(0, 23))).isoformat()
        upd_dt = (base_time + timedelta(days=random.randint(181, 360), hours=random.randint(0, 23))).isoformat()

        # Anomaly injection
        should_corrupt = (random.random() < error_rate)
        if should_corrupt and i > 5:  # Keep first few rows guaranteed valid
            anomaly_type = random.choice([
                "MALFORMED_CSV",
                "NULL_PRIMARY_KEY",
                "MISSING_FIELD",
                "INVALID_EMAIL",
                "INVALID_TIMESTAMP",
                "DUPLICATE_KEY",
            ])

            if anomaly_type == "MALFORMED_CSV":
                # Extra unescaped commas / unequal column count
                cust_rows.append([cust_id, first, last, "unclosed,quote,extra,column", email, phone])
                manifest["customers"]["breakdown"]["MALFORMED_CSV_ROW"] = manifest["customers"]["breakdown"].get("MALFORMED_CSV_ROW", 0) + 1
                manifest["customers"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "NULL_PRIMARY_KEY":
                cust_rows.append(["", first, last, email, phone, address, city, state, postal, country, reg_dt, upd_dt])
                manifest["customers"]["breakdown"]["NULL_PRIMARY_KEY"] = manifest["customers"]["breakdown"].get("NULL_PRIMARY_KEY", 0) + 1
                manifest["customers"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "MISSING_FIELD":
                # Missing required last_name
                cust_rows.append([cust_id, first, "", email, phone, address, city, state, postal, country, reg_dt, upd_dt])
                manifest["customers"]["breakdown"]["MISSING_REQUIRED_FIELD"] = manifest["customers"]["breakdown"].get("MISSING_REQUIRED_FIELD", 0) + 1
                manifest["customers"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "INVALID_EMAIL":
                bad_email = f"invalid-email-no-at-sign-{i}"
                cust_rows.append([cust_id, first, last, bad_email, phone, address, city, state, postal, country, reg_dt, upd_dt])
                manifest["customers"]["breakdown"]["INVALID_EMAIL_FORMAT"] = manifest["customers"]["breakdown"].get("INVALID_EMAIL_FORMAT", 0) + 1
                manifest["customers"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "INVALID_TIMESTAMP":
                bad_dt = "NOT-A-REAL-TIMESTAMP"
                cust_rows.append([cust_id, first, last, email, phone, address, city, state, postal, country, bad_dt, upd_dt])
                manifest["customers"]["breakdown"]["INVALID_TIMESTAMP"] = manifest["customers"]["breakdown"].get("INVALID_TIMESTAMP", 0) + 1
                manifest["customers"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "DUPLICATE_KEY":
                # Duplicate an existing valid customer ID with older timestamp
                dup_id = random.choice(valid_customer_ids) if valid_customer_ids else cust_id
                older_upd = (base_time - timedelta(days=10)).isoformat()
                cust_rows.append([dup_id, first, last, email, phone, address, city, state, postal, country, reg_dt, older_upd])
                manifest["customers"]["expected_duplicates"] += 1
                continue

        # Valid customer
        valid_customer_ids.append(cust_id)
        all_generated_cust_ids.append(cust_id)
        cust_rows.append([cust_id, first, last, email, phone, address, city, state, postal, country, reg_dt, upd_dt])
        manifest["customers"]["expected_valid"] += 1

    # 2. Generate Orders
    ord_rows: List[List[str]] = []
    ord_header = [
        "order_id", "customer_id", "order_date", "order_status",
        "total_amount", "currency", "payment_method", "item_count", "updated_at"
    ]

    valid_order_ids: List[str] = []

    for j in range(1, num_orders + 1):
        ord_id = f"ORD-{j:06d}"
        cust_id = random.choice(valid_customer_ids) if valid_customer_ids else "CUST-00001"
        ord_dt = (base_time + timedelta(days=random.randint(1, 300), hours=random.randint(0, 23))).isoformat()
        status = random.choice(ORDER_STATUSES)
        amount = round(random.uniform(5.00, 750.00), 2)
        currency = "USD"
        pm = random.choice(PAYMENT_METHODS)
        items = random.randint(1, 8)
        upd_dt = (base_time + timedelta(days=random.randint(301, 365), hours=random.randint(0, 23))).isoformat()

        should_corrupt = (random.random() < error_rate)
        if should_corrupt and j > 5:
            anomaly_type = random.choice([
                "MALFORMED_CSV",
                "NULL_PRIMARY_KEY",
                "ORPHAN_CUSTOMER",
                "NEGATIVE_AMOUNT",
                "INVALID_STATUS",
                "INVALID_ITEM_COUNT",
                "DUPLICATE_KEY",
            ])

            if anomaly_type == "MALFORMED_CSV":
                ord_rows.append([ord_id, cust_id, "broken_syntax_line", "extra_col_here"])
                manifest["orders"]["breakdown"]["MALFORMED_CSV_ROW"] = manifest["orders"]["breakdown"].get("MALFORMED_CSV_ROW", 0) + 1
                manifest["orders"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "NULL_PRIMARY_KEY":
                ord_rows.append(["", cust_id, ord_dt, status, str(amount), currency, pm, str(items), upd_dt])
                manifest["orders"]["breakdown"]["NULL_PRIMARY_KEY"] = manifest["orders"]["breakdown"].get("NULL_PRIMARY_KEY", 0) + 1
                manifest["orders"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "ORPHAN_CUSTOMER":
                orphan_cust_id = "CUST-99999"  # Does not exist
                ord_rows.append([ord_id, orphan_cust_id, ord_dt, status, str(amount), currency, pm, str(items), upd_dt])
                manifest["orders"]["breakdown"]["ORPHAN_FOREIGN_KEY"] = manifest["orders"]["breakdown"].get("ORPHAN_FOREIGN_KEY", 0) + 1
                manifest["orders"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "NEGATIVE_AMOUNT":
                neg_amount = round(-1 * random.uniform(10.0, 100.0), 2)
                ord_rows.append([ord_id, cust_id, ord_dt, status, str(neg_amount), currency, pm, str(items), upd_dt])
                manifest["orders"]["breakdown"]["NEGATIVE_OR_ZERO_AMOUNT"] = manifest["orders"]["breakdown"].get("NEGATIVE_OR_ZERO_AMOUNT", 0) + 1
                manifest["orders"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "INVALID_STATUS":
                ord_rows.append([ord_id, cust_id, ord_dt, "UNKNOWN_BOGUS_STATUS", str(amount), currency, pm, str(items), upd_dt])
                manifest["orders"]["breakdown"]["INVALID_ORDER_STATUS"] = manifest["orders"]["breakdown"].get("INVALID_ORDER_STATUS", 0) + 1
                manifest["orders"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "INVALID_ITEM_COUNT":
                ord_rows.append([ord_id, cust_id, ord_dt, status, str(amount), currency, pm, "0", upd_dt])
                manifest["orders"]["breakdown"]["INVALID_ITEM_COUNT"] = manifest["orders"]["breakdown"].get("INVALID_ITEM_COUNT", 0) + 1
                manifest["orders"]["expected_quarantined"] += 1
                continue

            elif anomaly_type == "DUPLICATE_KEY":
                dup_id = random.choice(valid_order_ids) if valid_order_ids else ord_id
                older_upd = (base_time - timedelta(days=15)).isoformat()
                ord_rows.append([dup_id, cust_id, ord_dt, status, str(amount), currency, pm, str(items), older_upd])
                manifest["orders"]["expected_duplicates"] += 1
                continue

        # Valid order
        valid_order_ids.append(ord_id)
        ord_rows.append([ord_id, cust_id, ord_dt, status, str(amount), currency, pm, str(items), upd_dt])
        manifest["orders"]["expected_valid"] += 1
        manifest["orders"]["expected_revenue_sum"] = round(manifest["orders"]["expected_revenue_sum"] + amount, 2)

    # Write CSV files
    cust_path = out_dir / "customers.csv"
    with open(cust_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cust_header)
        writer.writerows(cust_rows)

    ord_path = out_dir / "orders.csv"
    with open(ord_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(ord_header)
        writer.writerows(ord_rows)

    # Write ground-truth manifest
    m_path = Path(manifest_path)
    m_path.parent.mkdir(parents=True, exist_ok=True)
    with open(m_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return cust_path, ord_path, m_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic dataset generator for customers and orders")
    parser.add_argument("--customers", type=int, default=100, help="Number of customer records to generate")
    parser.add_argument("--orders", type=int, default=500, help="Number of order records to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--error-rate", type=float, default=0.06, help="Anomaly injection rate (0.0 to 1.0)")
    parser.add_argument("--output-dir", type=str, default="data/sample", help="Output directory for CSV files")
    parser.add_argument("--manifest", type=str, default="data/sample/ground_truth_manifest.json", help="Path for manifest")

    args = parser.parse_args()
    c_p, o_p, m_p = generate_datasets(
        num_customers=args.customers,
        num_orders=args.orders,
        seed=args.seed,
        error_rate=args.error_rate,
        output_dir=args.output_dir,
        manifest_path=args.manifest,
    )
    print(f"Generated {args.customers} customers -> {c_p}")
    print(f"Generated {args.orders} orders -> {o_p}")
    print(f"Ground truth manifest -> {m_p}")
