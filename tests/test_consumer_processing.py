import pytest


@pytest.mark.unit
class TestConsumerProcessing:

    def test_successful_payment_processing(self, consumer, payment_event_factory):
        """Verify normal payment event is processed and PaymentCompletedEvent is generated."""
        event = payment_event_factory(amount=500.0, source_account="ACC-A", destination_account="ACC-B")
        payload = event.model_dump_json()

        result = consumer.process_message(payload)
        assert result["status"] == "COMPLETED"
        assert result["transfer_id"] == event.transfer_id

        # Verify completed event list
        assert len(consumer.completed_events) == 1
        completed = consumer.completed_events[0]
        assert completed.transfer_id == event.transfer_id
        assert completed.amount == 500.0
        assert completed.fee == 0.75  # 0.15% of 500

        # Verify account ledger updates
        assert consumer.account_balances["ACC-A"] == 9500.0  # 10,000 - 500
        assert consumer.account_balances["ACC-B"] == 500.0
