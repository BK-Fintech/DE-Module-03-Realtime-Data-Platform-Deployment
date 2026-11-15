from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

-- 1. Table to record raw deduplicated events
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_time_utc TEXT NOT NULL,
    window_start_utc TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_utc TEXT NOT NULL
);

-- 2. Aggregation table by 1-minute time window
CREATE TABLE IF NOT EXISTS minute_metrics (
    window_start_utc TEXT PRIMARY KEY,
    order_count INTEGER NOT NULL DEFAULT 0,
    created_order_value_vnd INTEGER NOT NULL DEFAULT 0,
    product_view_count INTEGER NOT NULL DEFAULT 0,
    updated_at_utc TEXT NOT NULL
);

-- 3. Distinct customer tracking per window
CREATE TABLE IF NOT EXISTS customer_minute_activity (
    window_start_utc TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    first_seen_utc TEXT NOT NULL,
    PRIMARY KEY (window_start_utc, customer_id)
);

-- 4. Table for recording permanent invalid / rejected records
CREATE TABLE IF NOT EXISTS rejected_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset INTEGER NOT NULL,
    kafka_key TEXT,
    reason TEXT NOT NULL,
    raw_payload_base64 TEXT,
    rejected_at_utc TEXT NOT NULL,
    UNIQUE (topic, kafka_partition, kafka_offset)
);

-- 5. Analytical View for Dashboard
CREATE VIEW IF NOT EXISTS dashboard_minute AS
SELECT
    m.window_start_utc,
    m.order_count,
    m.created_order_value_vnd,
    m.product_view_count,
    COALESCE(c.active_customers, 0) AS active_customers,
    m.updated_at_utc
FROM minute_metrics m
LEFT JOIN (
    SELECT window_start_utc, COUNT(DISTINCT customer_id) AS active_customers
    FROM customer_minute_activity
    GROUP BY window_start_utc
) c ON m.window_start_utc = c.window_start_utc
ORDER BY m.window_start_utc DESC;
"""


class AnalyticsStore:
    """Manages transactional SQLite storage for streaming analytics."""

    def __init__(self, db_path: str = "data/realtime_analytics.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self) -> None:
        """Applies initial schema idempotently."""
        with self.get_connection() as conn:
            conn.executescript(SCHEMA_SQL)
            conn.commit()
        logger.info("Database schema initialized at %s", self.db_path)

    def record_rejected(
        self,
        topic: str,
        partition: int,
        offset: int,
        key: Optional[str],
        reason: str,
        raw_payload_b64: Optional[str],
    ) -> bool:
        """Records an invalid / poisoned event into rejected_events table."""
        now_utc = datetime.now(timezone.utc).isoformat()
        sql = """
        INSERT OR IGNORE INTO rejected_events (
            topic, kafka_partition, kafka_offset, kafka_key, reason, raw_payload_base64, rejected_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        with self.get_connection() as conn:
            conn.execute(sql, (topic, partition, offset, key, reason, raw_payload_b64, now_utc))
            conn.commit()
        return True

    def process_event_transactional(
        self,
        event: Dict[str, Any],
        window_utc: str,
        topic: str,
        partition: int,
        offset: int,
    ) -> bool:
        """Processes a single event transactionally with idempotency.

        Returns:
            True if event was newly applied.
            False if event was a duplicate (metrics unchanged).
        """
        event_id = event["event_id"]
        event_type = event["event_type"]
        customer_id = event["customer_id"]
        event_time_utc = event["event_time"]
        now_utc = datetime.now(timezone.utc).isoformat()
        payload_str = json.dumps(event, ensure_ascii=False, sort_keys=True)

        conn = self.get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE;")

            # Check if event_id already exists (Deduplication)
            cursor = conn.execute(
                "SELECT payload_json FROM processed_events WHERE event_id = ?",
                (event_id,),
            )
            existing = cursor.fetchone()

            if existing is not None:
                # Event already processed! Verify payload consistency
                if existing["payload_json"] != payload_str:
                    logger.warning(
                        "Data Quality Alert: Duplicate event_id %s received with conflicting payload!",
                        event_id,
                    )
                else:
                    logger.info("Duplicate event %s received. Skipping metric increments.", event_id)
                conn.execute("COMMIT;")
                return False

            # 1. Insert into processed_events
            conn.execute(
                """
                INSERT INTO processed_events (
                    event_id, event_type, event_time_utc, window_start_utc,
                    customer_id, topic, kafka_partition, kafka_offset,
                    payload_json, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    event_id,
                    event_type,
                    event_time_utc,
                    window_utc,
                    customer_id,
                    topic,
                    partition,
                    offset,
                    payload_str,
                    now_utc,
                ),
            )

            # 2. Track distinct customer activity for this window
            conn.execute(
                """
                INSERT OR IGNORE INTO customer_minute_activity (
                    window_start_utc, customer_id, first_seen_utc
                ) VALUES (?, ?, ?);
                """,
                (window_utc, customer_id, now_utc),
            )

            # 3. UPSERT minute_metrics
            if event_type == "OrderCreated":
                order_inc = 1
                value_inc = int(event["total_amount"])
                view_inc = 0
            elif event_type == "ProductViewed":
                order_inc = 0
                value_inc = 0
                view_inc = 1
            else:
                order_inc = 0
                value_inc = 0
                view_inc = 0

            conn.execute(
                """
                INSERT INTO minute_metrics (
                    window_start_utc, order_count, created_order_value_vnd, product_view_count, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(window_start_utc) DO UPDATE SET
                    order_count = order_count + excluded.order_count,
                    created_order_value_vnd = created_order_value_vnd + excluded.created_order_value_vnd,
                    product_view_count = product_view_count + excluded.product_view_count,
                    updated_at_utc = excluded.updated_at_utc;
                """,
                (window_utc, order_inc, value_inc, view_inc, now_utc),
            )

            conn.execute("COMMIT;")
            return True

        except Exception as err:
            conn.execute("ROLLBACK;")
            logger.error("Transaction failed for event %s: %s", event_id, err)
            raise
        finally:
            conn.close()

    def get_dashboard_metrics(self) -> List[Dict[str, Any]]:
        """Returns rows from the dashboard_minute view."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM dashboard_minute ORDER BY window_start_utc ASC;")
            return [dict(row) for row in cursor.fetchall()]
