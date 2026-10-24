from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field, field_serializer, field_validator


def to_iso_z(value: Optional[datetime]) -> Optional[str]:
    """Format a datetime as UTC ISO-8601 with a trailing 'Z' (e.g. 2024-01-15T10:30:00Z)."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class TransactionWebhook(BaseModel):
    transaction_id: str = Field(..., min_length=1, max_length=128)
    source_account: str = Field(..., min_length=1, max_length=128)
    destination_account: str = Field(..., min_length=1, max_length=128)
    amount: Decimal = Field(..., gt=0, max_digits=18, decimal_places=2)
    currency: str = Field(..., min_length=3, max_length=3)

    @field_validator("transaction_id", "source_account", "destination_account")
    @classmethod
    def strip_ids(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        value = value.strip().upper()
        if not value.isalpha():
            raise ValueError("must be a 3-letter ISO 4217 code")
        return value


class WebhookAck(BaseModel):
    message: str
    transaction_id: str
    duplicate: bool


class HealthResponse(BaseModel):
    status: str
    current_time: str


class TransactionOut(BaseModel):
    transaction_id: str
    source_account: str
    destination_account: str
    amount: Decimal
    currency: str
    status: str
    created_at: datetime
    processed_at: Optional[datetime]

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> float:
        return float(value)

    @field_serializer("created_at", "processed_at")
    def serialize_time(self, value: Optional[datetime]) -> Optional[str]:
        return to_iso_z(value)
