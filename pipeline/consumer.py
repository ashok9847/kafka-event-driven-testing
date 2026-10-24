import json
import logging
import uuid
from typing import Dict, Set, Optional, List
from pipeline.events import (
    PaymentInitiatedEvent,
    PaymentCompletedEvent,
    DeadLetterQueueEvent,
)

logger = logging.getLogger(__name__)


class UnrecoverableError(Exception):
    """Errors that should NOT be retried (poison pills, schema violations)."""
    pass


class TransientError(Exception):
    """Temporary errors eligible for retry (e.g. downstream timeout, locking contention)."""
    pass


class PaymentConsumerProcessor:
    """Event-Driven Kafka Consumer with deduplication, retry policy, and Dead-Letter Queue."""

    def __init__(
        self,
        max_retries: int = 3,
        completed_topic: str = "financial.payments.completed",
        dlq_topic: str = "financial.payments.dlq",
    ):
        self.max_retries = max_retries
        self.completed_topic = completed_topic
        self.dlq_topic = dlq_topic

        # Stateful components for test assertions
        self.processed_transfer_ids: Set[str] = set()
        self.completed_events: List[PaymentCompletedEvent] = []
        self.dlq_events: List[DeadLetterQueueEvent] = []
        self.account_balances: Dict[str, float] = {}

    def process_message(
        self,
        raw_payload: str,
        topic: str = "financial.payments.initiated",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """Core consumer handler executed for each Kafka record."""
        headers = headers or {}
        transfer_id = None
        retry_count = int(headers.get("x-retry-count", "0"))

        # Step 1: Payload Deserialization & Schema Contract Validation
        try:
            parsed = json.loads(raw_payload)
            event = PaymentInitiatedEvent.model_validate(parsed)
            transfer_id = event.transfer_id
        except Exception as exc:
            # Poison pill / Unrecoverable schema error -> Route directly to DLQ
            logger.error(f"Unrecoverable poison pill message: {exc}")
            self._route_to_dlq(
                transfer_id=None,
                original_topic=topic,
                raw_payload=raw_payload,
                error_reason=f"SchemaValidationFailure: {str(exc)}",
                retry_count=retry_count,
                headers=headers,
            )
            return {"status": "DLQ_POISON_PILL"}

        # Step 2: Idempotency & Deduplication Check
        if transfer_id in self.processed_transfer_ids:
            logger.warning(f"Duplicate event ignored for transfer_id: {transfer_id}")
            return {"status": "SKIPPED_DUPLICATE", "transfer_id": transfer_id}

        # Step 3: Business Logic & Simulated Failure Handling
        try:
            # Simulated transient failure for test scenarios
            if event.source_account.startswith("FAIL_TRANSIENT"):
                if retry_count < self.max_retries:
                    raise TransientError(f"Simulated database lock on account {event.source_account}")
                else:
                    raise UnrecoverableError("Exhausted maximum retry budget")

            # Simulated fatal validation error
            if event.source_account.startswith("FAIL_FATAL"):
                raise UnrecoverableError(f"Account {event.source_account} is permanently closed")

            # Execute transaction logic
            self._execute_transfer(event)
            self.processed_transfer_ids.add(transfer_id)

            # Produce Completed Event
            completed = PaymentCompletedEvent(
                event_id=f"EVT-COMPLETED-{uuid.uuid4().hex[:8].upper()}",
                transfer_id=event.transfer_id,
                source_account=event.source_account,
                destination_account=event.destination_account,
                amount=event.amount,
                currency=event.currency,
                fee=round(event.amount * 0.0015, 2),  # 0.15% fee
            )
            self.completed_events.append(completed)
            logger.info(f"Payment {transfer_id} successfully processed and emitted to {self.completed_topic}")
            return {"status": "COMPLETED", "transfer_id": transfer_id}

        except TransientError as exc:
            # Increment retry counter
            new_retry_count = retry_count + 1
            if new_retry_count >= self.max_retries:
                logger.error(f"Max retries ({self.max_retries}) reached for {transfer_id}. Routing to DLQ.")
                self._route_to_dlq(
                    transfer_id=transfer_id,
                    original_topic=topic,
                    raw_payload=raw_payload,
                    error_reason=f"MaxRetriesExceeded: {str(exc)}",
                    retry_count=new_retry_count,
                    headers=headers,
                )
                return {"status": "DLQ_RETRIES_EXHAUSTED", "transfer_id": transfer_id}
            else:
                logger.info(f"Retrying message {transfer_id} (Attempt {new_retry_count}/{self.max_retries})")
                return {
                    "status": "RETRY_SCHEDULED",
                    "transfer_id": transfer_id,
                    "next_retry_count": str(new_retry_count),
                }

        except UnrecoverableError as exc:
            logger.error(f"Fatal error processing {transfer_id}: {exc}. Routing to DLQ.")
            self._route_to_dlq(
                transfer_id=transfer_id,
                original_topic=topic,
                raw_payload=raw_payload,
                error_reason=f"FatalProcessingError: {str(exc)}",
                retry_count=retry_count,
                headers=headers,
            )
            return {"status": "DLQ_FATAL_ERROR", "transfer_id": transfer_id}

    def _execute_transfer(self, event: PaymentInitiatedEvent):
        """Simulate atomic double-entry balance mutation."""
        current_src = self.account_balances.get(event.source_account, 10000.0)
        current_dst = self.account_balances.get(event.destination_account, 0.0)
        self.account_balances[event.source_account] = round(current_src - event.amount, 2)
        self.account_balances[event.destination_account] = round(current_dst + event.amount, 2)

    def _route_to_dlq(
        self,
        transfer_id: Optional[str],
        original_topic: str,
        raw_payload: str,
        error_reason: str,
        retry_count: int,
        headers: Dict[str, str],
    ):
        dlq_event = DeadLetterQueueEvent(
            transfer_id=transfer_id,
            original_topic=original_topic,
            raw_payload=raw_payload,
            error_reason=error_reason,
            retry_count=retry_count,
            headers=headers,
        )
        self.dlq_events.append(dlq_event)
        logger.warning(f"Message routed to DLQ topic '{self.dlq_topic}'. Reason: {error_reason}")
