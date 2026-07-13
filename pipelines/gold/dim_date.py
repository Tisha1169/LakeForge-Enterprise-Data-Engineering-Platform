"""Generated calendar dimension — not sourced from any Silver table, just
computed for a fixed range. Regenerating it is always a full, deterministic
overwrite (there's nothing to merge)."""

from __future__ import annotations

from datetime import date, timedelta

DEFAULT_START = date(2023, 1, 1)
DEFAULT_END = date(2026, 12, 31)


def build_dim_date(start: date = DEFAULT_START, end: date = DEFAULT_END) -> list[dict]:
    rows = []
    current = start
    while current <= end:
        iso_weekday = current.isoweekday()  # 1=Monday .. 7=Sunday
        rows.append(
            {
                "date_key": int(current.strftime("%Y%m%d")),
                "full_date": current,
                "year": current.year,
                "quarter": (current.month - 1) // 3 + 1,
                "month": current.month,
                "month_name": current.strftime("%B"),
                "day": current.day,
                "day_of_week": iso_weekday,
                "day_name": current.strftime("%A"),
                "is_weekend": iso_weekday in (6, 7),
            }
        )
        current += timedelta(days=1)
    return rows
