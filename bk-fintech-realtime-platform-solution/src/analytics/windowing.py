from datetime import datetime, timezone
from typing import Union


def window_start_utc(event_time: Union[str, datetime]) -> str:
    """Converts a timezone-aware ISO-8601 timestamp to UTC and floors it to a 1-minute window.

    Output format: YYYY-MM-DDTHH:MM:00Z

    Raises:
        ValueError: If event_time is naive (lacks timezone) or malformed.
    """
    if isinstance(event_time, str):
        time_str = event_time.strip()
        if time_str.endswith("Z"):
            time_str = time_str[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(time_str)
        except Exception as err:
            raise ValueError(f"Malformed ISO-8601 timestamp '{event_time}': {err}") from err
    elif isinstance(event_time, datetime):
        dt = event_time
    else:
        raise TypeError(f"Expected str or datetime, got {type(event_time).__name__}")

    # Reject timezone-naive datetime
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError(f"Timestamp '{event_time}' is naive. Timezone information is strictly required.")

    # Convert to UTC
    dt_utc = dt.astimezone(timezone.utc)

    # Floor to minute (seconds=0, microseconds=0)
    window_dt = dt_utc.replace(second=0, microsecond=0)
    return window_dt.strftime("%Y-%m-%dT%H:%M:00Z")
