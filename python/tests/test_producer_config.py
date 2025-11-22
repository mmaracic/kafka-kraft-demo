from __future__ import annotations

from python.src.kafka_producer import ProducerConfig


def test_direct_construction_and_alias_dump() -> None:
    cfg = ProducerConfig(
        bootstrap_servers="broker:9092",
        topic="test-topic",
        client_id="demo-client",
        linger_ms=5,
        acks="1",
    )

    # attributes accessible by python field names
    assert cfg.bootstrap_servers == "broker:9092"
    assert cfg.topic == "test-topic"

    dumped = cfg.model_dump()
    # model_dump defaults to by_alias so alias keys should be present
    assert "bootstrap.servers" in dumped
    assert dumped["bootstrap.servers"] == "broker:9092"


def test_wrapper_constructor_extracts_nested_node() -> None:
    payload = {
        "kafka-producer": {
            "bootstrap.servers": "broker:9092",
            "topic": "test-topic",
            "client.id": "demo-client",
            "linger.ms": 10,
            "acks": "all",
        }
    }

    # use the wrapper keys configured on ProducerConfig
    cfg = ProducerConfig(source=payload)

    assert cfg.bootstrap_servers == "broker:9092"
    assert cfg.linger_ms == 10
    dumped = cfg.model_dump()
    assert dumped["topic"] == "test-topic"
