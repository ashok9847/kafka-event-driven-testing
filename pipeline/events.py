from datetime import datetime, timezone
from typing import Dict, Optional
from pydantic import BaseModel, Field


class PaymentInitiatedEvent(BaseModel):
    event_id: str
    transfer_id: str
    source_account: str
    destination_account: str
    amount: float = Field(..., gt=0.0)
    currency: str = Field("USD", pattern=r"^[A-Z]{3}$")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, str] = Field(default_factory=dict)


class PaymentCompletedEvent(BaseModel):
    event_id: str
    transfer_id: str
    source_account: str
    destination_account: str
    amount: float
    currency: str
    status: str = "COMPLETED"
    fee: float = 0.0
    completed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DeadLetterQueueEvent(BaseModel):
    transfer_id: Optional[str] = None
    original_topic: str
    raw_payload: str
    error_reason: str
    retry_count: int
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    headers: Dict[str, str] = Field(default_factory=dict)
