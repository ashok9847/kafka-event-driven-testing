import pytest
from pipeline.events import PaymentInitiatedEvent


@pytest.mark.unit
class TestProducerEvents:

    def test_producer_publishes_with_partition_key(self, producer, payment_event_factory):
        """Verify the producer uses transfer_id as message key for partition pinning."""
        event = payment_event_factory()
        record = producer.publish_payment_initiated(event)

        assert record["key"] == event.transfer_id
        assert record["topic"] == "financial.payments.initiated"
        assert record["value"]["amount"] == event.amount
        assert record["value"]["transfer_id"] == event.transfer_id

    def test_producer_attaches_idempotency_headers(self, producer, payment_event_factory):
        """Verify required metadata headers (idempotency key and schema version) are present."""
        event = payment_event_factory()
        record = producer.publish_payment_initiated(event)

        headers = record["headers"]
        assert headers["x-idempotency-key"] == event.transfer_id
        assert headers["x-schema-version"] == "1.0.0"
        assert headers["x-source"] == "payment-gateway"

    def test_invalid_negative_amount_raises_validation_error(self):
        """Pydantic validation must reject negative payment amounts before publishing."""
        with pytest.raises(Exception):
            PaymentInitiatedEvent(
                event_id="EVT-1",
                transfer_id="TRF-1",
                source_account="ACC-1",
                destination_account="ACC-2",
                amount=-100.0,
                currency="USD",
            )
