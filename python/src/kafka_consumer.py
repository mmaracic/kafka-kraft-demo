from __future__ import annotations

import logging
from collections.abc import Generator
from typing import Any, ClassVar, Optional

from confluent_kafka import Consumer as ConfluentConsumer
from pydantic import ConfigDict, Field
from src.config import BaseConfig
from src.model import MessageModel

logger = logging.getLogger(__name__)


class ConsumerConfig(BaseConfig):
    """Configuration for Kafka consumer, mirrors producer-style alias names."""

    WRAPPER_DATA_PATH: ClassVar[Optional[str]] = "kafka-consumer"

    model_config = ConfigDict(extra="ignore")

    bootstrap_servers: str = Field(alias="bootstrap.servers")
    group_id: str = Field(alias="group.id")
    topic: str = Field(alias="topic", exclude=True)
    auto_offset_reset: str = Field(alias="auto.offset.reset")
    enable_auto_commit: bool = Field(alias="enable.auto.commit")


class KafkaConsumer:
    """Consumer wrapper that yields MessageModel objects via a generator.

    Instantiate with a `ConsumerConfig`, then iterate over `messages(timeout)`:

        for msg in consumer.messages(timeout=1.0):
            handle(msg)

    Call `close()` to stop the generator and close the consumer.
    """

    def __init__(self, config: ConsumerConfig) -> None:
        """Initialize the Kafka consumer from a validated `ConsumerConfig`."""
        # pass confluent-kafka configuration as alias-keyed mapping
        self._consumer: ConfluentConsumer = ConfluentConsumer(
            config.model_dump(by_alias=True),
        )
        self._consumer.subscribe([config.topic])

    def messages(
        self, timeout: float, auto_stop: bool
    ) -> Generator[MessageModel, None, None]:
        """Yield MessageModel instances as messages arrive.

        Continue looping until `close()` is called. If no message is available
        within `timeout`, the poll returns `None` and the loop continues.
        """
        try:
            while True:
                msg = self._consumer.poll(timeout)
                if msg is None:
                    if auto_stop:
                        logger.info(
                            "No message received within timeout, stopping consumer"
                        )
                        break
                    continue
                if msg.error():
                    logger.error("Consumer error: %s", msg.error())
                    continue
                try:
                    payload = (
                        msg.value().decode("utf-8")
                        if isinstance(msg.value(), (bytes, bytearray))
                        else msg.value()
                    )
                    model = (
                        MessageModel.model_validate_json(payload)
                        if isinstance(payload, str)
                        else MessageModel.model_validate(payload)
                    )
                    yield model
                    logger.info("Committed consumption of message: %s", model)
                    self._consumer.commit(message=msg, asynchronous=True)
                except Exception:
                    logger.exception("Failed to parse message payload")
                    continue
        finally:
            try:
                self._consumer.close()
                logger.info("Kafka consumer closed")
            except Exception:
                logger.exception("Error closing consumer")
