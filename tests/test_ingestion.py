"""Tests for CSV ingestion, malformed row quarantine, and schema inspection."""

from pathlib import Path
import pytest

from src.pipeline.reader import read_csv_safely, SchemaMismatchError
from src.pipeline.schemas import CUSTOMER_REQUIRED_COLUMNS, ORDER_REQUIRED_COLUMNS, FailureCode


def test_ingest_happy_path(tmp_path: Path):
    file_path = tmp_path / "valid_customers.csv"
    file_path.write_text(
        "customer_id,first_name,last_name,email,phone,address,city,state,postal_code,country,registration_date,updated_at\n"
        "CUST-001,John,Doe,john.doe@example.com,1234567890,123 Main,SF,CA,94105,US,2024-01-01T00:00:00Z,2024-01-02T00:00:00Z\n"
    )

    res = read_csv_safely(
        file_path=file_path,
        table_name="customers",
        required_columns=CUSTOMER_REQUIRED_COLUMNS,
        run_id="run_1",
    )

    assert res.total_rows_read == 1
    assert len(res.records) == 1
    assert len(res.quarantined) == 0
    assert res.records[0]["customer_id"] == "CUST-001"
    assert res.records[0]["first_name"] == "John"


def test_ingest_empty_input(tmp_path: Path):
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("")

    res = read_csv_safely(
        file_path=empty_file,
        table_name="customers",
        required_columns=CUSTOMER_REQUIRED_COLUMNS,
        run_id="run_empty",
    )

    assert res.total_rows_read == 0
    assert len(res.records) == 0
    assert len(res.quarantined) == 0


def test_ingest_whitespace_only_file(tmp_path: Path):
    ws_file = tmp_path / "whitespace.csv"
    ws_file.write_text(
        "customer_id,first_name,last_name,email,phone,address,city,state,postal_code,country,registration_date,updated_at\n"
        "   \n"
        "\n"
        "     \n"
    )

    res = read_csv_safely(
        file_path=ws_file,
        table_name="customers",
        required_columns=CUSTOMER_REQUIRED_COLUMNS,
        run_id="run_ws",
    )

    assert res.total_rows_read == 0
    assert len(res.records) == 0
    assert len(res.quarantined) == 0


def test_ingest_malformed_csv_row_extra_columns(tmp_path: Path):
    bad_csv = tmp_path / "bad_columns.csv"
    bad_csv.write_text(
        "customer_id,first_name,last_name,email,phone,address,city,state,postal_code,country,registration_date,updated_at\n"
        "CUST-001,John,Doe,john@example.com,123,Street,City,CA,94105,US,2024-01-01T00:00:00Z,2024-01-02T00:00:00Z\n"
        "CUST-002,Jane,Doe,jane@example.com,EXTRA_COLUMN_A,EXTRA_COLUMN_B,123,Street,City,CA,94105,US,2024-01-01T00:00:00Z,2024-01-02T00:00:00Z\n"
    )

    res = read_csv_safely(
        file_path=bad_csv,
        table_name="customers",
        required_columns=CUSTOMER_REQUIRED_COLUMNS,
        run_id="run_malformed",
    )

    assert res.total_rows_read == 2
    assert len(res.records) == 1
    assert len(res.quarantined) == 1
    assert res.quarantined[0].failure_code == FailureCode.MALFORMED_CSV_ROW.value
    assert res.quarantined[0].source_row_number == 3
    assert res.quarantined[0].record_id == "CUST-002"


def test_ingest_unexpected_schema_missing_columns(tmp_path: Path):
    missing_header_csv = tmp_path / "wrong_schema.csv"
    missing_header_csv.write_text(
        "customer_id,first_name,some_random_column\n"
        "CUST-001,John,RandomValue\n"
    )

    with pytest.raises(SchemaMismatchError) as exc_info:
        read_csv_safely(
            file_path=missing_header_csv,
            table_name="customers",
            required_columns=CUSTOMER_REQUIRED_COLUMNS,
            run_id="run_schema_err",
            strict_schema=True,
        )

    assert "Missing required columns in CSV header" in str(exc_info.value)
