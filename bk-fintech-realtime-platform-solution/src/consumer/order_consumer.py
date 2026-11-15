import argparse
import base64
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.common.serialization import deserialize_event
from src.common.validation import ValidationError, validate_order_created

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("order_consumer")


class OrderConsumer:
    """Kafka Consumer implementing strict manual commit and quarantine envelope routing."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str = "order-events",
        output_dir: str = "data",
        crash_after_write_event_id: Optional[str] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.output_dir = Path(output_dir)
        self.crash_after_write_event_id = crash_after_write_event_id

        # Setup output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.processed_path = self.output_dir / "processed.jsonl"
        self.quarantine_path = self.output_dir / "quarantine.jsonl"

        # Kafka Consumer configuration
        # auto.offset.reset=earliest ensures new groups replay retained events
        # enable.auto.commit=False ensures manual atomic durability
        self.conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        }
        self.consumer = Consumer(self.conf)
        self.running = True

    def _append_to_file(self, file_path: Path, data: Dict[str, Any]) -> None:
        """Appends a single JSON envelope to JSONL and calls fsync for disk durability."""
        line = json.dumps(data, ensure_ascii=False) + "\n"
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def process_message(self, msg: Any) -> bool:
        """Processes a single Kafka message.

        Returns True if offset should be committed, False if infrastructure error.
        """
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        raw_key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
        raw_value = msg.value()

        # Step 1: Decode and Validate
        is_valid = False
        quarantine_reason = None
        event_payload = None

        try:
            event_payload = deserialize_event(raw_value)
            validate_order_created(event_payload)
            is_valid = True
        except (ValueError, ValidationError) as err:
            # Bad data format or business violation -> Quarantine
            is_valid = False
            quarantine_reason = str(err)
            logger.warning(
                "Invalid record detected at [%s:%d@%d] (Key=%s). Reason: %s",
                topic,
                partition,
                offset,
                raw_key,
                quarantine_reason,
            )
        except Exception as err:
            # Unexpected system / internal error -> DO NOT COMMIT
            logger.critical("Unexpected error while parsing: %s. Offset will NOT be committed.", err)
            return False

        # Step 2: Write envelope to sink
        now_utc = datetime.now(timezone.utc).isoformat()

        if is_valid:
            event_id = event_payload["event_id"]
            envelope = {
                "status": "PROCESSED",
                "processed_at": now_utc,
                "kafka": {
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "key": raw_key,
                },
                "event_id": event_id,
                "payload": event_payload,
            }

            try:
                self._append_to_file(self.processed_path, envelope)
                logger.info(
                    "[PERSISTED] Event %s (Order: %s) to %s [%s:%d@%d]",
                    event_id,
                    event_payload.get("order_id"),
                    self.processed_path.name,
                    topic,
                    partition,
                    offset,
                )
            except IOError as io_err:
                logger.error("Disk I/O error writing processed envelope: %s. Offset uncommitted.", io_err)
                return False

            # LAB TEST HOOK: Simulate crash after disk append but before commit
            if self.crash_after_write_event_id and self.crash_after_write_event_id == event_id:
                logger.critical(
                    "*** SIMULATED CRASH TRIGGERED: Event %s appended to disk, CRASHING BEFORE COMMIT! ***",
                    event_id,
                )
                # Hard exit immediately
                os._exit(42)

        else:
            # Quarantine envelope with safe base64 representation of original payload
            b64_raw = base64.b64encode(raw_value).decode("ascii") if raw_value else None
            envelope = {
                "status": "QUARANTINED",
                "quarantined_at": now_utc,
                "kafka": {
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "key": raw_key,
                },
                "reason": quarantine_reason,
                "raw_payload_base64": b64_raw,
            }

            try:
                self._append_to_file(self.quarantine_path, envelope)
                logger.info(
                    "[QUARANTINED] Message at [%s:%d@%d] saved to %s",
                    topic,
                    partition,
                    offset,
                    self.quarantine_path.name,
                )
            except IOError as io_err:
                logger.error("Disk I/O error writing quarantine envelope: %s. Offset uncommitted.", io_err)
                return False

        # Step 3: Synchronously commit offset
        try:
            self.consumer.commit(message=msg, asynchronous=False)
            logger.info("[OFFSET_COMMITTED] Successfully committed offset %d on partition %d", offset, partition)
            return True
        except KafkaException as commit_err:
            logger.error("Failed to commit offset: %s", commit_err)
            return False

    def start(self, max_records: Optional[int] = None, idle_timeout: float = 15.0) -> None:
        """Starts consumption loop. Stops on SIGINT/Ctrl+C or when idle_timeout expires in test mode."""
        self.consumer.subscribe([self.topic])
        logger.info(
            "Consumer group '%s' subscribed to topic '%s'. Listening...",
            self.group_id,
            self.topic,
        )

        records_processed = 0
        last_record_time = time.time()

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # No message available in poll interval
                    if max_records and (time.time() - last_record_time > idle_timeout):
                        logger.info("Idle timeout reached (%s s). Exiting.", idle_timeout)
                        break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition: %s [%d]", msg.topic(), msg.partition())
                    else:
                        logger.error("Kafka error received: %s", msg.error())
                    continue

                # Process message
                last_record_time = time.time()
                success = self.process_message(msg)
                if not success:
                    raise RuntimeError(
                        f"Failed to process {msg.topic()}[{msg.partition()}]@{msg.offset()}; "
                        "stopping before a later commit can skip this offset"
                    )
                records_processed += 1

                if max_records and records_processed >= max_records:
                    logger.info("Processed target count (%d records). Stopping loop.", max_records)
                    break

        except KeyboardInterrupt:
            logger.info("Termination signal received. Closing consumer...")
        finally:
            self.consumer.close()
            logger.info("Consumer closed. Total records processed this session: %d", records_processed)


def main():
    parser = argparse.ArgumentParser(description="Consume OrderCreated events and append to JSONL.")
    parser.add_argument("--group", type=str, required=True, help="Kafka consumer group ID")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory for JSONL files")
    parser.add_argument("--topic", type=str, default="order-events", help="Kafka topic to consume from")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka broker address")
    parser.add_argument(
        "--crash-after-write-event-id",
        type=str,
        default=None,
        help="[TEST-ONLY] Crash process after appending this specific event ID before offset commit",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Optional: Stop after processing N records (useful for automated testing)",
    )

    args = parser.parse_args()

    consumer = OrderConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group,
        topic=args.topic,
        output_dir=args.output_dir,
        crash_after_write_event_id=args.crash_after_write_event_id,
    )
    consumer.start(max_records=args.max_records)


if __name__ == "__main__":
    main()
