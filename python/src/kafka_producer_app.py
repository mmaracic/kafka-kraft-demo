from __future__ import annotations

from pathlib import Path
from typing import Any
import logging

import yaml
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from src.kafka_producer import ProducerConfig, KafkaProducer

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


@app.on_event("startup")
def startup_event() -> None:
    global _producer
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
    for i in range(n):
        try:
            _producer.produce(req.text, timeout=1.0)
            sent += 1
        except Exception as exc:
            logger.exception("Failed to produce message #%d: %s", i, exc)
            break

    return {"topic": _producer._config.topic if _producer else None, "sent": sent}


@app.get("/")
def root() -> dict[str, str]:
    return {"status": "ok", "service": "kafka-producer"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
