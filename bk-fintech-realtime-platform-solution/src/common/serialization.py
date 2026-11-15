import json
from typing import Any, Dict


def serialize_event(event_data: Dict[str, Any]) -> bytes:
    """Serialize dictionary event to UTF-8 encoded JSON bytes.

    Ensures Vietnamese / Unicode characters are preserved correctly without ascii escaping.
    """
    if not isinstance(event_data, dict):
        raise TypeError(f"Expected dict, got {type(event_data).__name__}")
    return json.dumps(event_data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def deserialize_event(raw_bytes: bytes) -> Dict[str, Any]:
    """Deserialize raw bytes into a Python dictionary.

    Raises:
        ValueError: If input is not valid UTF-8 or not valid JSON object.
    """
    if not isinstance(raw_bytes, (bytes, bytearray)):
        raise TypeError(f"Expected bytes, got {type(raw_bytes).__name__}")

    try:
        decoded_text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as err:
        raise ValueError(f"Invalid UTF-8 payload: {err}") from err

    try:
        data = json.loads(decoded_text)
    except json.JSONDecodeError as err:
        raise ValueError(f"Invalid JSON format: {err}") from err

    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object (dict), got {type(data).__name__}")

    return data
