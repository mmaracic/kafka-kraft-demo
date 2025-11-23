from __future__ import annotations

from pathlib import Path
from typing import Any
import logging

import yaml
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from src.kafka_producer import ProducerConfig, KafkaProducer
from src.kafka_consumer import ConsumerConfig, KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_producer_app")


class ProduceRequest(BaseModel):
    text: str


def load_config_from_yaml(path: Path) -> dict[str, Any]:
    content = path.read_text()
    data = yaml.safe_load(content)
    if not isinstance(data, dict):
        raise ValueError("config yaml must contain a mapping at top level")
    return data


app = FastAPI(title="Kafka Producer API")

# app state
_producer: KafkaProducer | None = None
_consumer: KafkaConsumer | None = None


@app.on_event("startup")
def startup_event() -> None:
    global _producer, _consumer
    try:
        cfg_path = Path(__file__).resolve().parents[1] / "config.yaml"
        logger.info("Loading config from %s", cfg_path)
        cfg_data = load_config_from_yaml(cfg_path)

        # ProducerConfig is configured to extract its node from the wrapper
        # (it expects constructor invoked with key 'source'). We pass the
        # top-level mapping so the model will pick the configured sub-dict.
        config = ProducerConfig(source=cfg_data)
        logger.info("ProducerConfig loaded for topic %s", config.topic)

        _producer = KafkaProducer(config)

        # Initialize consumer from YAML consumer section if present
        consumer_cfg = ConsumerConfig(source=cfg_data)
        logger.info("ConsumerConfig loaded for topic %s", consumer_cfg.topic)
        _consumer = KafkaConsumer(consumer_cfg)
    except Exception as exc:
        logger.exception("Failed to initialize Kafka producer: %s", exc)
        # leave _producer as None; endpoints will return 503


@app.post("/produce")
def produce_messages(
    req: ProduceRequest,
    n: int = Query(1, ge=1, le=1000, description="Number of messages to send"),
) -> dict[str, Any]:
    """Send `n` messages to Kafka with the provided text payload.

    Returns a simple summary including how many messages were accepted.
    """
    if _producer is None:
        raise HTTPException(status_code=503, detail="Producer not initialized")

    sent = 0
    try:
        for _ in range(n):
            _producer.produce(req.text, timeout=1.0)
            sent += 1
    except Exception as exc:
        logger.exception("Unexpected error during message production: %s", exc)

    return {"Messages sent": sent}


@app.get("/")
def root() -> dict[str, str]:
    return {"status": "ok", "service": "kafka-producer"}


@app.get("/consume")
def consume_messages(
    max_messages: int = Query(10, ge=1, le=1000, description="Max messages to consume"),
) -> dict[str, Any]:
    """Consume up to `max_messages` from the configured consumer.

    If the consumer was configured with `enable.auto.commit: false`, the
    endpoint will explicitly commit each message after processing.
    """
    if _consumer is None:
        raise HTTPException(status_code=503, detail="Consumer not initialized")

    messages: list[Any] = []
    consumed = 0

    for message in _consumer.messages(timeout=1.0, auto_stop=True):
        messages.append(message)
        logger.info("Consumed message: %s", message)
        consumed += 1
        if consumed >= max_messages:
            break

    return {
        "consumed": consumed,
        "messages": messages,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
