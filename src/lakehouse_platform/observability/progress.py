"""Human-readable progress events for notebooks, jobs and local demos."""
from __future__ import annotations

from datetime import datetime, timezone


def progress(component: str, message: str, **details) -> None:
    """Print one compact, consistently formatted pipeline event."""
    timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
    suffix = " ".join(f"{key}={value}" for key, value in details.items())
    line = f"[{timestamp}] [{component}] {message}"
    print(f"{line} | {suffix}" if suffix else line)
