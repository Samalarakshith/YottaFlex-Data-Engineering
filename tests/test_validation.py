"""Unit tests for schema validation and domain business rules for customers and orders."""

from src.pipeline.schemas import FailureCode
from src.pipeline.validator import validate_customers, validate_orders


def test_customer_null_primary_key():
    raw = [{
        "customer_id": "",
        "first_name": "Alice",
        "last_name": "Smith",
        "email": "alice@example.com",
        "country": "US",
        "registration_date": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-02T00:00:00Z",
        "_source_row_number": "2",
        "_source_file": "cust.csv",
    }]
    valid, quarantined = validate_customers(raw, run_id="r1")
    assert len(valid) == 0
    assert len(quarantined) == 1
    assert quarantined[0].failure_code == FailureCode.NULL_PRIMARY_KEY.value


def test_customer_missing_required_field():
    raw = [{
        "customer_id": "CUST-001",
        "first_name": "Alice",
        "last_name": "",  # missing required
        "email": "alice@example.com",
        "country": "US",
        "registration_date": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-02T00:00:00Z",
        "_source_row_number": "2",
        "_source_file": "cust.csv",
    }]
    valid, quarantined = validate_customers(raw, run_id="r1")
    assert len(valid) == 0
    assert len(quarantined) == 1
    assert quarantined[0].failure_code == FailureCode.MISSING_REQUIRED_FIELD.value


def test_customer_invalid_email_format():
    invalid_emails = ["not-an-email", "user@", "@domain.com", "user space@domain.com"]
    for bad_email in invalid_emails:
        raw = [{
            "customer_id": "CUST-001",
            "first_name": "Alice",
            "last_name": "Smith",
            "email": bad_email,
            "country": "US",
            "registration_date": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "_source_row_number": "2",
            "_source_file": "cust.csv",
        }]
        valid, quarantined = validate_customers(raw, run_id="r1")
        assert len(valid) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_code == FailureCode.INVALID_EMAIL_FORMAT.value


def test_customer_invalid_timestamp():
    raw = [{
        "customer_id": "CUST-001",
        "first_name": "Alice",
        "last_name": "Smith",
        "email": "alice@example.com",
        "country": "US",
        "registration_date": "INVALID_DATE_FORMAT",
        "updated_at": "2024-01-02T00:00:00Z",
        "_source_row_number": "2",
        "_source_file": "cust.csv",
    }]
    valid, quarantined = validate_customers(raw, run_id="r1")
    assert len(valid) == 0
    assert len(quarantined) == 1
    assert quarantined[0].failure_code == FailureCode.INVALID_TIMESTAMP.value


def test_order_null_primary_key():
    raw = [{
        "order_id": "",
        "customer_id": "CUST-001",
        "order_date": "2024-01-10T12:00:00Z",
        "order_status": "DELIVERED",
        "total_amount": "99.50",
        "currency": "USD",
        "payment_method": "credit_card",
        "item_count": "2",
        "updated_at": "2024-01-10T12:00:00Z",
        "_source_row_number": "2",
        "_source_file": "orders.csv",
    }]
    valid, quarantined = validate_orders(raw, run_id="r1")
    assert len(valid) == 0
    assert len(quarantined) == 1
    assert quarantined[0].failure_code == FailureCode.NULL_PRIMARY_KEY.value


def test_order_negative_and_zero_amount():
    for bad_amt in ["0.00", "-15.50", "-0.01"]:
        raw = [{
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "order_date": "2024-01-10T12:00:00Z",
            "order_status": "DELIVERED",
            "total_amount": bad_amt,
            "currency": "USD",
            "payment_method": "credit_card",
            "item_count": "2",
            "updated_at": "2024-01-10T12:00:00Z",
            "_source_row_number": "2",
            "_source_file": "orders.csv",
        }]
        valid, quarantined = validate_orders(raw, run_id="r1")
        assert len(valid) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_code == FailureCode.NEGATIVE_OR_ZERO_AMOUNT.value


def test_order_boundary_amount_valid():
    raw = [{
        "order_id": "ORD-001",
        "customer_id": "CUST-001",
        "order_date": "2024-01-10T12:00:00Z",
        "order_status": "DELIVERED",
        "total_amount": "0.01",  # Exactly min boundary
        "currency": "USD",
        "payment_method": "credit_card",
        "item_count": "1",
        "updated_at": "2024-01-10T12:00:00Z",
        "_source_row_number": "2",
        "_source_file": "orders.csv",
    }]
    valid, quarantined = validate_orders(raw, run_id="r1")
    assert len(valid) == 1
    assert len(quarantined) == 0
    assert valid[0]["total_amount"] == 0.01


def test_order_exceeds_max_amount():
    raw = [{
        "order_id": "ORD-001",
        "customer_id": "CUST-001",
        "order_date": "2024-01-10T12:00:00Z",
        "order_status": "DELIVERED",
        "total_amount": "1000001.00",  # Over 1,000,000 limit
        "currency": "USD",
        "payment_method": "credit_card",
        "item_count": "1",
        "updated_at": "2024-01-10T12:00:00Z",
        "_source_row_number": "2",
        "_source_file": "orders.csv",
    }]
    valid, quarantined = validate_orders(raw, run_id="r1")
    assert len(valid) == 0
    assert len(quarantined) == 1
    assert quarantined[0].failure_code == FailureCode.EXCEEDS_MAX_AMOUNT.value


def test_order_invalid_status():
    raw = [{
        "order_id": "ORD-001",
        "customer_id": "CUST-001",
        "order_date": "2024-01-10T12:00:00Z",
        "order_status": "FAKE_STATUS_FOOBAR",
        "total_amount": "50.00",
        "currency": "USD",
        "payment_method": "credit_card",
        "item_count": "1",
        "updated_at": "2024-01-10T12:00:00Z",
        "_source_row_number": "2",
        "_source_file": "orders.csv",
    }]
    valid, quarantined = validate_orders(raw, run_id="r1")
    assert len(valid) == 0
    assert len(quarantined) == 1
    assert quarantined[0].failure_code == FailureCode.INVALID_ORDER_STATUS.value


def test_order_invalid_item_count():
    for bad_item in ["0", "-2", "abc", "2.5"]:
        raw = [{
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "order_date": "2024-01-10T12:00:00Z",
            "order_status": "PENDING",
            "total_amount": "50.00",
            "currency": "USD",
            "payment_method": "credit_card",
            "item_count": bad_item,
            "updated_at": "2024-01-10T12:00:00Z",
            "_source_row_number": "2",
            "_source_file": "orders.csv",
        }]
        valid, quarantined = validate_orders(raw, run_id="r1")
        assert len(valid) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_code == FailureCode.INVALID_ITEM_COUNT.value
