import pytest


@pytest.mark.idempotency
class TestIdempotencyAndDeduplication:

    def test_duplicate_message_is_safely_ignored(self, consumer, payment_event_factory):
        """Simulate duplicate message redelivery (Kafka at-least-once guarantee)."""
        event = payment_event_factory(amount=1000.0, source_account="ACC-ORIG", destination_account="ACC-DEST")
        payload = event.model_dump_json()

        # First consumption -> Success
        res1 = consumer.process_message(payload)
        assert res1["status"] == "COMPLETED"
        assert consumer.account_balances["ACC-ORIG"] == 9000.0

        # Second consumption (duplicate redelivery) -> Must be SKIPPED
        res2 = consumer.process_message(payload)
        assert res2["status"] == "SKIPPED_DUPLICATE"

        # Third consumption -> Must also be SKIPPED
        res3 = consumer.process_message(payload)
        assert res3["status"] == "SKIPPED_DUPLICATE"

        # Crucial check: balance must NOT be debited more than once
        assert consumer.account_balances["ACC-ORIG"] == 9000.0
        assert consumer.account_balances["ACC-DEST"] == 1000.0
        assert len(consumer.completed_events) == 1
