from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, Optional
import json
import logging
import uuid

from pydantic import Field
from confluent_kafka import Producer as ConfluentProducer
from src.model import MessageModel
from src.config import BaseConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


class ProducerConfig(BaseConfig):
    # Enable wrapper-based constructor extraction using these keys
    WRAPPER_DATA_PATH: ClassVar[Optional[str]] = "kafka-producer"

    bootstrap_servers: str = Field(alias="bootstrap.servers")
    topic: str = Field(alias="topic")
    client_id: str = Field(alias="client.id")
    linger_ms: int = Field(alias="linger.ms")
    acks: str = Field(alias="acks")


class KafkaProducer:
    """Simple Kafka producer wrapper using confluent-kafka.

    This class requires a `ProducerConfig` and provides `produce` and
    `flush` methods. It uses a delivery callback for logging.
    """

    _producer: ConfluentProducer

    def __init__(self, config: ProducerConfig) -> None:
        self._config: ProducerConfig = config
        conf: Dict[str, Any] = {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": config.client_id,
            "linger.ms": config.linger_ms,
            "acks": config.acks,
        }
        self._producer: ConfluentProducer = ConfluentProducer(conf)

    def _delivery_report(self, err: Exception | None, msg: Any) -> None:
        if err is not None:
            logger.error("Delivery failed for message: %s", err)
        else:
            logger.debug(
                "Message delivered to %s [%s] at offset %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def produce(self, value: str, timeout: float) -> None:
        """Produce a JSON-encoded message.

        Args:
            key: partitioning key for the message
            value: JSON-serializable payload
            timeout: max time (seconds) to wait for producer poll/queueing
        """
        payload: str = json.dumps(
            MessageModel(
                id=str(uuid.uuid4()),
                content=value,
                timestamp=datetime.now(timezone.utc).isoformat(),
            ).model_dump()
        )
        self._producer.produce(
            topic=self._config.topic,
            key=None,
            value=payload,
            callback=self._delivery_report,
        )
        # serve delivery callbacks and queue until message is accepted
        self._producer.poll(timeout)

    def flush(self, timeout: float) -> None:
        """Block until all messages are delivered or timeout (seconds) elapses."""
        self._producer.flush(timeout)
