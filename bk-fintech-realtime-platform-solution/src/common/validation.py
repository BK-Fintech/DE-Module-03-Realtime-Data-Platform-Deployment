from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict
import jsonschema


class ValidationError(Exception):
    """Custom exception raised when an event violates schema or business logic."""

    def __init__(self, message: str, error_type: str = "VALIDATION_ERROR"):
        super().__init__(message)
        self.error_type = error_type


_SCHEMA_CACHE: Dict[str, Dict[str, Any]] = {}


def load_schema(schema_name: str) -> Dict[str, Any]:
    """Loads and caches a JSON schema from the contracts directory."""
    if schema_name not in _SCHEMA_CACHE:
        current_dir = Path(__file__).resolve().parent
        repo_root = current_dir.parent.parent
        schema_path = repo_root / "contracts" / schema_name

        if not schema_path.exists():
            raise FileNotFoundError(f"Schema not found at {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as f:
            _SCHEMA_CACHE[schema_name] = json.load(f)

    return _SCHEMA_CACHE[schema_name]


def validate_order_created(event: Dict[str, Any]) -> None:
    """Validates an OrderCreated event against both JSON Schema and business invariants.

    Business Invariants:
    1. Boolean values must not be accepted for integer fields.
    2. total_amount must exactly equal sum(item['quantity'] * item['unit_price']).
    3. event_time must be a valid ISO-8601 UTC timestamp.
    """
    if not isinstance(event, dict):
        raise ValidationError(f"Event must be a dict, got {type(event).__name__}", "TYPE_ERROR")

    # 1. JSON Schema Structural Validation
    schema = load_schema("order_created.schema.json")
    try:
        jsonschema.validate(instance=event, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        raise ValidationError(f"Schema validation failed: {err.message}", "SCHEMA_VIOLATION") from err

    # 2. Strict Type Check: Reject booleans in integer fields
    if isinstance(event.get("total_amount"), bool):
        raise ValidationError("Field 'total_amount' cannot be boolean", "TYPE_ERROR")
    if isinstance(event.get("event_version"), bool):
        raise ValidationError("Field 'event_version' cannot be boolean", "TYPE_ERROR")

    items = event.get("items", [])
    computed_total = 0
    for idx, item in enumerate(items):
        qty = item.get("quantity")
        price = item.get("unit_price")

        if isinstance(qty, bool) or isinstance(price, bool):
            raise ValidationError(f"Item #{idx} contains boolean in numeric fields", "TYPE_ERROR")

        computed_total += qty * price

    # 3. Business Invariant: Total Amount calculation check
    if event.get("total_amount") != computed_total:
        raise ValidationError(
            f"Business invariant failed: total_amount ({event.get('total_amount')}) "
            f"does not match sum of items ({computed_total})",
            "INVARIANT_VIOLATION",
        )

    # 4. UTC Timestamp Check
    event_time_str = event.get("event_time", "")
    try:
        if not (event_time_str.endswith("Z") or event_time_str.endswith("+00:00")):
            raise ValidationError("event_time must be UTC format (ending in Z or +00:00)", "TIMESTAMP_ERROR")
        datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))
    except Exception as err:
        raise ValidationError(f"Invalid timestamp format: {err}", "TIMESTAMP_ERROR") from err


def validate_product_viewed(event: Dict[str, Any]) -> None:
    """Validates a ProductViewed event against JSON Schema and UTC timestamp rule."""
    if not isinstance(event, dict):
        raise ValidationError(f"Event must be a dict, got {type(event).__name__}", "TYPE_ERROR")

    schema = load_schema("product_viewed.schema.json")
    try:
        jsonschema.validate(instance=event, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        raise ValidationError(f"Schema validation failed: {err.message}", "SCHEMA_VIOLATION") from err

    if isinstance(event.get("event_version"), bool):
        raise ValidationError("Field 'event_version' cannot be boolean", "TYPE_ERROR")

    event_time_str = event.get("event_time", "")
    try:
        if not (event_time_str.endswith("Z") or event_time_str.endswith("+00:00")):
            raise ValidationError("event_time must be UTC format (ending in Z or +00:00)", "TIMESTAMP_ERROR")
        datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))
    except Exception as err:
        raise ValidationError(f"Invalid timestamp format: {err}", "TIMESTAMP_ERROR") from err
