from datetime import datetime, timezone
from typing import Optional


def build_s3_key(
    topic: str,
    partition: int,
    offset: int,
    event_time: Optional[str] = None,
) -> str:
    """Builds deterministic S3 object key according to data contract:

    raw/topic=<topic>/dt=<UTC-event-date>/partition=<partition>/offset=<zero-padded-offset>.json
    If event_time is missing or invalid, stores under unknown/ prefix.
    """
    offset_padded = f"{offset:020d}"

    if not event_time:
        return f"raw/invalid/topic={topic}/partition={partition}/offset={offset_padded}.json"

    try:
        time_str = event_time.strip()
        if time_str.endswith("Z"):
            time_str = time_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(time_str).astimezone(timezone.utc)
        dt_str = dt.strftime("%Y-%m-%d")
        return f"raw/topic={topic}/dt={dt_str}/partition={partition}/offset={offset_padded}.json"
    except Exception:
        return f"raw/invalid/topic={topic}/partition={partition}/offset={offset_padded}.json"
