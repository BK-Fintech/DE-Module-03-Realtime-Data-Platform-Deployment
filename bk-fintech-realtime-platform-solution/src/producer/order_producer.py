import logging
from typing import Any, Callable, Dict, Optional
from confluent_kafka import KafkaException, Producer

logger = logging.getLogger(__name__)


class OrderProducer:
    """Wrapper around confluent_kafka.Producer with delivery callbacks and tracking."""

    def __init__(self, bootstrap_servers: str = "localhost:9092", client_id: str = "order-producer-client"):
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 200,
        }
        self.producer = Producer(self.config)
        self.delivered_count = 0
        self.failed_count = 0

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback triggered by poll() or flush() once Kafka broker responds."""
        if err is not None:
            self.failed_count += 1
            logger.error("Message delivery failed: %s (Key: %s)", err, msg.key())
        else:
            self.delivered_count += 1
            logger.debug(
                "Message delivered to %s [%s] @ offset %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        callback: Optional[Callable[[Any, Any], None]] = None,
    ) -> None:
        """Publishes a single message to Kafka.

        Key is mandatory to ensure consistent partition hashing.
        """
        cb = callback or self._delivery_callback
        try:
            self.producer.produce(
                topic=topic,
                key=key.encode("utf-8") if isinstance(key, str) else key,
                value=value,
                on_delivery=cb,
            )
            # Serve delivery reports for previous messages
            self.producer.poll(0)
        except KafkaException as err:
            self.failed_count += 1
            logger.error("Failed to produce message: %s", err)
            raise

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for all outstanding messages to be delivered.

        Returns remaining un-delivered message count (0 means clean).
        """
        return self.producer.flush(timeout)

    def stats(self) -> Dict[str, int]:
        """Returns delivery counts."""
        return {
            "delivered": self.delivered_count,
            "failed": self.failed_count,
        }
