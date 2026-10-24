import pytest


@pytest.mark.dlq
class TestRetryAndDeadLetterQueue:

    def test_corrupted_json_poison_pill_routed_to_dlq(self, consumer):
        """A corrupted payload must be captured directly into DLQ without crashing consumer loop."""
        malformed_raw = '{"transfer_id": "TRF-CORRUPT", "amount": "NOT_A_NUMBER"'  # Invalid JSON syntax

        result = consumer.process_message(malformed_raw)
        assert result["status"] == "DLQ_POISON_PILL"

        assert len(consumer.dlq_events) == 1
        dlq_event = consumer.dlq_events[0]
        assert "SchemaValidationFailure" in dlq_event.error_reason
        assert dlq_event.raw_payload == malformed_raw

    def test_transient_failure_retries_and_exhausts_to_dlq(self, consumer, payment_event_factory):
        """Simulate transient error retrying up to max_retries before routing to DLQ."""
        event = payment_event_factory(source_account="FAIL_TRANSIENT_ACCOUNT")
        payload = event.model_dump_json()

        # Attempt 1: retry 0 -> schedules retry 1
        step1 = consumer.process_message(payload, headers={"x-retry-count": "0"})
        assert step1["status"] == "RETRY_SCHEDULED"
        assert step1["next_retry_count"] == "1"
        assert len(consumer.dlq_events) == 0

        # Attempt 2: retry 1 -> schedules retry 2
        step2 = consumer.process_message(payload, headers={"x-retry-count": "1"})
        assert step2["status"] == "RETRY_SCHEDULED"
        assert step2["next_retry_count"] == "2"
        assert len(consumer.dlq_events) == 0

        # Attempt 3: retry 2 -> reaches max (3) -> routes to DLQ
        step3 = consumer.process_message(payload, headers={"x-retry-count": "2"})
        assert step3["status"] == "DLQ_RETRIES_EXHAUSTED"
        assert len(consumer.dlq_events) == 1

        dlq_item = consumer.dlq_events[0]
        assert dlq_item.transfer_id == event.transfer_id
        assert dlq_item.retry_count == 3
        assert "MaxRetriesExceeded" in dlq_item.error_reason

    def test_fatal_business_error_routes_to_dlq_immediately(self, consumer, payment_event_factory):
        """Permanent non-retryable failures bypass retries directly to DLQ."""
        event = payment_event_factory(source_account="FAIL_FATAL_ACCOUNT")
        payload = event.model_dump_json()

        result = consumer.process_message(payload)
        assert result["status"] == "DLQ_FATAL_ERROR"
        assert len(consumer.dlq_events) == 1
        assert "permanently closed" in consumer.dlq_events[0].error_reason
