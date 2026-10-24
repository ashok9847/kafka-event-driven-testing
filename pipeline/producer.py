import json
import logging
from typing import Optional, Dict, Any
from pipeline.events import PaymentInitiatedEvent

logger = logging.getLogger(__name__)


class PaymentProducer:
    """Kafka Producer for publishing payment events with deterministic partition keys."""

    def __init__(self, bootstrap_servers: Optional[str] = None, kafka_producer=None):
        self.bootstrap_servers = bootstrap_servers
        self.producer = kafka_producer
        self.published_messages = []  # In-memory tracking for test observability

    def publish_payment_initiated(
        self,
        event: PaymentInitiatedEvent,
        topic: str = "financial.payments.initiated",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Publishes payment event with idempotency headers and transfer_id partition key."""
        payload_bytes = event.model_dump_json().encode("utf-8")
        msg_key = event.transfer_id.encode("utf-8")

        message_headers = {
            "x-idempotency-key": event.transfer_id,
            "x-schema-version": "1.0.0",
            "x-source": "payment-gateway",
            **(headers or {}),
        }

        # If live confluent-kafka producer is present:
        if self.producer:
            kafka_headers = [(k, v.encode("utf-8")) for k, v in message_headers.items()]
            self.producer.produce(
                topic=topic,
                key=msg_key,
                value=payload_bytes,
                headers=kafka_headers,
            )
            self.producer.flush(timeout=5)

        record = {
            "topic": topic,
            "key": event.transfer_id,
            "value": json.loads(payload_bytes.decode("utf-8")),
            "headers": message_headers,
        }
        self.published_messages.append(record)
        logger.info(f"Published PaymentInitiatedEvent: {event.transfer_id} to {topic}")
        return record
