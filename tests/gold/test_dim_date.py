from datetime import date

from pipelines.gold.dim_date import build_dim_date


def test_build_dim_date_covers_full_range_inclusive():
    rows = build_dim_date(date(2024, 1, 1), date(2024, 1, 3))
    assert len(rows) == 3
    assert [r["date_key"] for r in rows] == [20240101, 20240102, 20240103]


def test_build_dim_date_flags_weekend_correctly():
    # 2024-01-06 is a Saturday, 2024-01-07 a Sunday, 2024-01-08 a Monday.
    rows = build_dim_date(date(2024, 1, 6), date(2024, 1, 8))
    by_date = {r["full_date"]: r for r in rows}
    assert by_date[date(2024, 1, 6)]["is_weekend"] is True
    assert by_date[date(2024, 1, 7)]["is_weekend"] is True
    assert by_date[date(2024, 1, 8)]["is_weekend"] is False
    assert by_date[date(2024, 1, 8)]["day_name"] == "Monday"
