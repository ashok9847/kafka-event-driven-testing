import uuid
import pytest
from faker import Faker
from pipeline.producer import PaymentProducer
from pipeline.consumer import PaymentConsumerProcessor
from pipeline.events import PaymentInitiatedEvent

fake = Faker()


@pytest.fixture
def producer():
    """Returns an initialized PaymentProducer."""
    return PaymentProducer()


@pytest.fixture
def consumer():
    """Returns an isolated consumer processor with default retry limits."""
    return PaymentConsumerProcessor(max_retries=3)


@pytest.fixture
def payment_event_factory():
    """Generates unique, realistic PaymentInitiatedEvents."""
    def _create(
        source_account: str = "ACC-1001",
        destination_account: str = "ACC-2002",
        amount: float = 250.0,
        currency: str = "USD",
    ) -> PaymentInitiatedEvent:
        transfer_id = f"TRF-{uuid.uuid4().hex[:10].upper()}"
        return PaymentInitiatedEvent(
            event_id=f"EVT-INIT-{uuid.uuid4().hex[:8].upper()}",
            transfer_id=transfer_id,
            source_account=source_account,
            destination_account=destination_account,
            amount=amount,
            currency=currency,
            metadata={"channel": "mobile-banking", "client_ip": fake.ipv4()},
        )
    return _create
