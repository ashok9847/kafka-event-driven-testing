# Kafka Event-Driven Testing Framework

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.0-black.svg)](https://kafka.apache.org/)
[![Testcontainers](https://img.shields.io/badge/Testcontainers-Kafka-1F8EFA.svg)](https://testcontainers.com/)
[![pytest](https://img.shields.io/badge/pytest-8.2.0-blue.svg)](https://docs.pytest.org/)
[![CI Status](https://img.shields.io/badge/CI-Passing-brightgreen.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A test automation framework engineered for **Apache Kafka event-driven architectures**. Demonstrates end-to-end integration testing, idempotent deduplication, retry policies, poison pill resilience, and **Dead-Letter Queue (DLQ)** routing in high-throughput financial pipelines.

Developed by **[Ashok Aryal](https://github.com/ashok9847)**.

---

## 1. Architecture & Event Pipeline Flow

In financial systems, message queues must handle network drops, duplicate deliveries, and poison pill payloads without halting consumer loops or double-charging accounts.

```
                             +-------------------------------+
                             |    Payment Event Producer     |
                             |  - Key: transfer_id           |
                             |  - Header: x-idempotency-key  |
                             +---------------+---------------+
                                             |
                                  Publishes to Topic
                                             v
                             +-------------------------------+
                             |  financial.payments.initiated |
                             +---------------+---------------+
                                             |
                                    Consumed by Worker
                                             v
                      +---------------------------------------------+
                      |         Payment Consumer Processor          |
                      |  1. JSON Deserialization & Pydantic Schema  |
                      |  2. Idempotency Check (Deduplication)       |
                      |  3. Double-Entry Balance Mutation           |
                      |  4. Retry Backoff Engine (Max 3 Attempts)   |
                      +------+-------------------------------+------+
                             |                               |
                 Success     |                               |   Failure / Poison Pill
                             v                               v
       +-------------------------------+      +-------------------------------+
       | financial.payments.completed  |      |    financial.payments.dlq     |
       | - Status: COMPLETED           |      | - x-retry-count: 3            |
       | - Transaction Fee Recorded    |      | - x-error-reason: [Detail]    |
       +-------------------------------+      +-------------------------------+
```

---

## 2. Key Testing Capabilities

* **Deduplication & Idempotency Testing (`test_idempotency_dedup.py`)**:
  Simulates Kafka's at-least-once delivery guarantees by bombarding the consumer with duplicated event messages. Verifies that balances are debited strictly once and duplicate events are skipped.
* **Poison Pill Isolation (`test_retry_and_dlq.py`)**:
  Sends malformed JSON payloads and invalid schema fields. Verifies that corrupt events are safely segregated to the DLQ with error metadata rather than crashing the consumer thread.
* **Retry Exhaustion to DLQ (`test_retry_and_dlq.py`)**:
  Simulates transient network/database timeout failures. Tests the step-by-step retry progression (`0 -> 1 -> 2 -> 3`) and validates that upon exceeding the retry budget, the message is routed to the DLQ with full diagnostic headers.
* **Deterministic Partition Key Hashing (`test_producer_events.py`)**:
  Verifies that producers attach `transfer_id` as the message key to guarantee order preservation across identical accounts.

---

## 3. Directory Structure

```
kafka-event-driven-testing/
├── .github/
│   └── workflows/
│       └── ci.yml               # GitHub Actions CI with KRaft Kafka service
├── pipeline/
│   ├── __init__.py
│   ├── events.py                # Event schemas (Initiated, Completed, DLQ)
│   ├── producer.py              # Kafka payment producer with header management
│   └── consumer.py              # Consumer with retry backoff & DLQ logic
├── tests/
│   ├── __init__.py
│   ├── conftest.py              # Test fixtures and Faker event generators
│   ├── test_producer_events.py  # Message key & header assertions
│   ├── test_consumer_processing.py # Happy-path transaction processing
│   ├── test_idempotency_dedup.py # Duplicate delivery and deduplication tests
│   └── test_retry_and_dlq.py    # Poison pill & retry exhaustion tests
├── docker-compose.yml           # Ephemeral Kafka broker (KRaft mode) + runner
├── pytest.ini                   # Pytest markers and configuration
├── requirements.txt             # Dependencies
└── README.md
```

---

## 4. Quickstart & Local Execution

### Local Python Virtualenv
```bash
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On Linux/macOS:
# source .venv/bin/activate

pip install -r requirements.txt
```

### Run Tests
```bash
# Run all tests
pytest -v

# Run only Dead-Letter Queue tests
pytest -m dlq

# Run only Idempotency & Deduplication tests
pytest -m idempotency
```

### Run with Docker Compose
```bash
docker compose up --build --abort-on-container-exit
```

---

## 5. Interview-Ready Technical Concepts

### Q1: What is a "Poison Pill" message in Kafka, and how does this test framework guard against it?
> **Answer**: A poison pill is a message that cannot be deserialized or processed due to schema violations, corrupted data, or unhandled exceptions. If unhandled, the consumer crashes, restarts, reads the same offset, and enters an infinite crash loop (consumer lag spikes to infinity). In this framework, `consumer.py` wraps deserialization in a protected block that routes unrecoverable errors directly to `financial.payments.dlq` without committing offsets until the DLQ write succeeds.

### Q2: How do you achieve Exactly-Once Processing in Kafka consumer architectures?
> **Answer**: Kafka provides at-least-once delivery by default. To achieve effective exactly-once semantics, applications implement idempotency. In our framework, consumers maintain a deduplication check (`processed_transfer_ids` or an atomic DB unique constraint table). When a duplicate message arrives from a consumer rebalance or producer retry, the consumer recognizes the `x-idempotency-key`, acknowledges the offset, and skips execution.

---

## License
MIT License. Created by [Ashok Aryal](https://github.com/ashok9847).
