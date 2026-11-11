"""Robust CSV ingestion module with malformed row handling, schema inspection, and quarantine routing."""

import csv
import io
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from src.pipeline.schemas import FailureCode, QuarantineRecord


class SchemaMismatchError(Exception):
    """Raised when source CSV header deviates critically from required schema."""
    pass


class IngestionResult:
    def __init__(
        self,
        table_name: str,
        source_path: Path,
        records: List[Dict[str, str]],
        quarantined: List[QuarantineRecord],
        total_rows_read: int,
        headers: List[str],
    ):
        self.table_name = table_name
        self.source_path = source_path
        self.records = records
        self.quarantined = quarantined
        self.total_rows_read = total_rows_read
        self.headers = headers


def read_csv_safely(
    file_path: Path | str,
    table_name: str,
    required_columns: Set[str],
    run_id: str,
    batch_id: str = "batch_1",
    strict_schema: bool = True,
) -> IngestionResult:
    """Safely reads a CSV file line by line, capturing malformed rows and schema violations.

    Args:
        file_path: Path to the CSV file.
        table_name: Logical table name ('customers' or 'orders').
        required_columns: Set of required column names.
        run_id: Pipeline run identifier.
        batch_id: Batch identifier.
        strict_schema: If True, raises SchemaMismatchError on missing required headers.

    Returns:
        IngestionResult containing valid raw dictionaries and quarantined malformed records.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    records: List[Dict[str, str]] = []
    quarantined: List[QuarantineRecord] = []
    headers: List[str] = []
    total_rows_read = 0

    now_iso = datetime.now(timezone.utc).isoformat()

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        # Check for empty file
        first_line = f.readline()
        if not first_line:
            return IngestionResult(
                table_name=table_name,
                source_path=path,
                records=[],
                quarantined=[],
                total_rows_read=0,
                headers=[],
            )

        # Parse header
        header_reader = csv.reader([first_line.strip()])
        try:
            headers = [h.strip() for h in next(header_reader)]
        except Exception as e:
            quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    batch_id=batch_id,
                    source_table=table_name,
                    source_file=str(path),
                    source_row_number=1,
                    record_id=None,
                    failure_code=FailureCode.UNEXPECTED_SCHEMA.value,
                    failure_reason=f"Failed to parse header line: {str(e)}",
                    raw_payload=first_line.strip(),
                    rejected_at=now_iso,
                )
            )
            return IngestionResult(table_name, path, [], quarantined, 0, [])

        # Validate header schema
        missing_columns = required_columns - set(headers)
        if missing_columns:
            reason = f"Missing required columns in CSV header: {sorted(list(missing_columns))}"
            if strict_schema:
                raise SchemaMismatchError(reason)
            else:
                quarantined.append(
                    QuarantineRecord(
                        quarantine_id=str(uuid.uuid4()),
                        run_id=run_id,
                        batch_id=batch_id,
                        source_table=table_name,
                        source_file=str(path),
                        source_row_number=1,
                        record_id=None,
                        failure_code=FailureCode.UNEXPECTED_SCHEMA.value,
                        failure_reason=reason,
                        raw_payload=first_line.strip(),
                        rejected_at=now_iso,
                    )
                )
                return IngestionResult(table_name, path, [], quarantined, 0, headers)

        expected_cols = len(headers)
        line_num = 1

        for raw_line in f:
            line_num += 1
            stripped = raw_line.strip()
            if not stripped:
                # Skip empty whitespace lines
                continue

            total_rows_read += 1

            try:
                row_reader = csv.reader([stripped])
                row = next(row_reader)
            except Exception as e:
                quarantined.append(
                    QuarantineRecord(
                        quarantine_id=str(uuid.uuid4()),
                        run_id=run_id,
                        batch_id=batch_id,
                        source_table=table_name,
                        source_file=str(path),
                        source_row_number=line_num,
                        record_id=None,
                        failure_code=FailureCode.MALFORMED_CSV_ROW.value,
                        failure_reason=f"Malformed CSV syntax: {str(e)}",
                        raw_payload=stripped,
                        rejected_at=now_iso,
                    )
                )
                continue

            # Check column count
            if len(row) != expected_cols:
                # Extract any possible key from first column if present
                rec_id = row[0].strip() if len(row) > 0 and row[0].strip() else None
                quarantined.append(
                    QuarantineRecord(
                        quarantine_id=str(uuid.uuid4()),
                        run_id=run_id,
                        batch_id=batch_id,
                        source_table=table_name,
                        source_file=str(path),
                        source_row_number=line_num,
                        record_id=rec_id,
                        failure_code=FailureCode.MALFORMED_CSV_ROW.value,
                        failure_reason=f"Column count mismatch: expected {expected_cols}, got {len(row)}",
                        raw_payload=stripped,
                        rejected_at=now_iso,
                    )
                )
                continue

            # Valid CSV row structure: convert to dict
            row_dict = {headers[i]: row[i].strip() for i in range(expected_cols)}
            row_dict["_source_row_number"] = str(line_num)
            row_dict["_source_file"] = str(path)
            row_dict["_raw_payload"] = stripped
            records.append(row_dict)

    return IngestionResult(
        table_name=table_name,
        source_path=path,
        records=records,
        quarantined=quarantined,
        total_rows_read=total_rows_read,
        headers=headers,
    )
