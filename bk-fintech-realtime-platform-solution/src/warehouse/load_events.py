import argparse
from datetime import datetime, timezone
import logging
import os
from typing import Any, Dict, List
import boto3
from botocore.client import Config
import clickhouse_connect

from src.common.serialization import deserialize_event
from src.common.validation import ValidationError, validate_order_created, validate_product_viewed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("warehouse_loader")


class WarehouseLoader:
    """Reads raw JSON objects from S3 Data Lake, validates schemas,

    and loads into ClickHouse analytical tables.
    """

    def __init__(
        self,
        s3_endpoint: str,
        bucket_name: str,
        clickhouse_host: str,
        clickhouse_port: int,
        clickhouse_db: str,
        clickhouse_user: str = "default",
        clickhouse_password: str = "",
        aws_access_key: str = "anykey",
        aws_secret_key: str = "anysecret",
    ):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
            region_name="us-east-1",
        )
        self.ch_client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            database=clickhouse_db,
            username=clickhouse_user,
            password=clickhouse_password,
        )

    def load_from_prefix(self, prefix: str = "raw/") -> Dict[str, int]:
        logger.info("Scanning S3 bucket '%s' for prefix '%s'...", self.bucket_name, prefix)
        paginator = self.s3_client.get_paginator("list_objects_v2")

        order_rows: List[List[Any]] = []
        view_rows: List[List[Any]] = []
        skipped_count = 0

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue

                try:
                    res = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                    raw_bytes = res["Body"].read()
                except Exception:
                    logger.exception("Could not read S3 object %s; aborting load for retry", key)
                    raise

                try:
                    meta = res.get("Metadata", {})

                    event_dict = deserialize_event(raw_bytes)
                    event_type = event_dict.get("event_type")

                    # Provenance from S3 metadata
                    kafka_topic = meta.get("kafka-topic", "unknown")
                    kafka_partition = int(meta.get("kafka-partition", 0))
                    kafka_offset = int(meta.get("kafka-offset", 0))

                    if event_type == "OrderCreated":
                        validate_order_created(event_dict)
                        dt_str = event_dict["event_time"].replace("Z", "+00:00")
                        dt_utc = datetime.fromisoformat(dt_str).astimezone(timezone.utc)
                        order_rows.append([
                            event_dict["event_id"],
                            dt_utc,
                            event_dict["order_id"],
                            event_dict["customer_id"],
                            int(event_dict["total_amount"]),
                            event_dict["currency"],
                            kafka_topic,
                            kafka_partition,
                            kafka_offset,
                        ])
                    elif event_type == "ProductViewed":
                        validate_product_viewed(event_dict)
                        dt_str = event_dict["event_time"].replace("Z", "+00:00")
                        dt_utc = datetime.fromisoformat(dt_str).astimezone(timezone.utc)
                        view_rows.append([
                            event_dict["event_id"],
                            dt_utc,
                            event_dict["customer_id"],
                            event_dict["product_id"],
                            event_dict.get("session_id", ""),
                            event_dict.get("device_type", "unknown"),
                            kafka_topic,
                            kafka_partition,
                            kafka_offset,
                        ])
                    else:
                        skipped_count += 1

                except (ValidationError, ValueError, TypeError, KeyError) as err:
                    logger.warning("Failed to parse/validate S3 object %s: %s", key, err)
                    skipped_count += 1

        # Batch Insert to ClickHouse
        if order_rows:
            order_cols = [
                "event_id",
                "event_time",
                "order_id",
                "customer_id",
                "total_amount",
                "currency",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
            ]
            self.ch_client.insert("orders", order_rows, column_names=order_cols)
            logger.info("Inserted %d order rows into analytics.orders", len(order_rows))

        if view_rows:
            view_cols = [
                "event_id",
                "event_time",
                "customer_id",
                "product_id",
                "session_id",
                "device_type",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
            ]
            self.ch_client.insert("product_views", view_rows, column_names=view_cols)
            logger.info("Inserted %d view rows into analytics.product_views", len(view_rows))

        summary = {
            "orders_loaded": len(order_rows),
            "views_loaded": len(view_rows),
            "skipped": skipped_count,
        }
        logger.info("--- LOAD SUMMARY --- %s", summary)
        return summary


def main():
    default_s3 = os.getenv("S3_ENDPOINT_URL", "http://object-storage:8333")
    default_bucket = os.getenv("S3_BUCKET_NAME", "ecommerce-raw")
    default_ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    default_ch_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    default_ch_db = os.getenv("CLICKHOUSE_DB", "analytics")
    default_ch_user = os.getenv("CLICKHOUSE_USER", "default")
    default_ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    default_access = os.getenv("AWS_ACCESS_KEY_ID", "anykey")
    default_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "anysecret")

    parser = argparse.ArgumentParser(description="Load S3 raw events to ClickHouse.")
    parser.add_argument("--prefix", type=str, default="raw/", help="S3 prefix to read from")
    parser.add_argument("--s3-endpoint", type=str, default=default_s3, help="S3 endpoint")
    parser.add_argument("--bucket", type=str, default=default_bucket, help="S3 bucket")
    parser.add_argument("--clickhouse-host", type=str, default=default_ch_host, help="ClickHouse host")
    parser.add_argument("--clickhouse-port", type=int, default=default_ch_port, help="ClickHouse HTTP port")
    parser.add_argument("--clickhouse-db", type=str, default=default_ch_db, help="ClickHouse database")
    parser.add_argument("--clickhouse-user", type=str, default=default_ch_user, help="ClickHouse user")
    parser.add_argument("--clickhouse-password", type=str, default=default_ch_password, help=argparse.SUPPRESS)

    args = parser.parse_args()

    loader = WarehouseLoader(
        s3_endpoint=args.s3_endpoint,
        bucket_name=args.bucket,
        clickhouse_host=args.clickhouse_host,
        clickhouse_port=args.clickhouse_port,
        clickhouse_db=args.clickhouse_db,
        clickhouse_user=args.clickhouse_user,
        clickhouse_password=args.clickhouse_password,
        aws_access_key=default_access,
        aws_secret_key=default_secret,
    )
    loader.load_from_prefix(prefix=args.prefix)


if __name__ == "__main__":
    main()
