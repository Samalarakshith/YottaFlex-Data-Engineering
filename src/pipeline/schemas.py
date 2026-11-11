"""Data Schemas, Type Definitions, and Validation Rules for Customers and Orders."""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Set
import re

# Regex for RFC-compliant email validation
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")


class FailureCode(str, Enum):
    MALFORMED_CSV_ROW = "MALFORMED_CSV_ROW"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    NULL_PRIMARY_KEY = "NULL_PRIMARY_KEY"
    INVALID_EMAIL_FORMAT = "INVALID_EMAIL_FORMAT"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    INVALID_NUMERIC = "INVALID_NUMERIC"
    NEGATIVE_OR_ZERO_AMOUNT = "NEGATIVE_OR_ZERO_AMOUNT"
    EXCEEDS_MAX_AMOUNT = "EXCEEDS_MAX_AMOUNT"
    INVALID_ITEM_COUNT = "INVALID_ITEM_COUNT"
    INVALID_ORDER_STATUS = "INVALID_ORDER_STATUS"
    UNEXPECTED_SCHEMA = "UNEXPECTED_SCHEMA"
    ORPHAN_FOREIGN_KEY = "ORPHAN_FOREIGN_KEY"
    DUPLICATE_KEY_QUARANTINED = "DUPLICATE_KEY_QUARANTINED"


CUSTOMER_SOURCE_COLUMNS: List[str] = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "address",
    "city",
    "state",
    "postal_code",
    "country",
    "registration_date",
    "updated_at",
]

CUSTOMER_REQUIRED_COLUMNS: Set[str] = {
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "country",
    "registration_date",
    "updated_at",
}

ORDER_SOURCE_COLUMNS: List[str] = [
    "order_id",
    "customer_id",
    "order_date",
    "order_status",
    "total_amount",
    "currency",
    "payment_method",
    "item_count",
    "updated_at",
]

ORDER_REQUIRED_COLUMNS: Set[str] = {
    "order_id",
    "customer_id",
    "order_date",
    "order_status",
    "total_amount",
    "currency",
    "payment_method",
    "item_count",
    "updated_at",
}

ALLOWED_ORDER_STATUSES: Set[str] = {
    "PENDING",
    "PROCESSING",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED",
}


@dataclass
class QuarantineRecord:
    quarantine_id: str
    run_id: str
    batch_id: str
    source_table: str
    source_file: str
    source_row_number: int
    record_id: Optional[str]
    failure_code: str
    failure_reason: str
    raw_payload: str
    rejected_at: str

    def to_dict(self) -> Dict:
        return {
            "quarantine_id": self.quarantine_id,
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "source_table": self.source_table,
            "source_file": self.source_file,
            "source_row_number": self.source_row_number,
            "record_id": self.record_id or "",
            "failure_code": self.failure_code,
            "failure_reason": self.failure_reason,
            "raw_payload": self.raw_payload,
            "rejected_at": self.rejected_at,
        }
