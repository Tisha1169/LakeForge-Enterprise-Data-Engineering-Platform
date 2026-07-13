from datetime import date

from moto import mock_aws
from pipelines.gold.dim_customer import build_dim_customer
from pipelines.gold.writer import write_gold_table
from pipelines.storage import LakeLayer, ensure_bucket

from tests.gold.helpers import seed_silver


def _customer(customer_id, loyalty_tier="bronze", email=None):
    return {
        "customer_id": customer_id,
        "email": email or f"customer{customer_id}@example.com",
        "first_name": "Alex",
        "last_name": "Smith",
        "loyalty_tier": loyalty_tier,
    }


@mock_aws
def test_bootstrap_run_makes_every_customer_current():
    batch1 = date(2024, 1, 1)
    seed_silver("customers", "customers", batch1, [_customer(1), _customer(2)])

    rows = build_dim_customer(batch1)

    assert len(rows) == 2
    assert all(r["is_current"] for r in rows)
    assert all(r["end_date"] is None for r in rows)
    assert all(r["effective_date"] == batch1 for r in rows)


@mock_aws
def test_second_run_expires_changed_customer_and_adds_new_one():
    ensure_bucket(LakeLayer.GOLD)
    batch1 = date(2024, 1, 1)
    seed_silver(
        "customers", "customers", batch1, [_customer(1, loyalty_tier="bronze"), _customer(2)]
    )
    write_gold_table("dim_customer", build_dim_customer(batch1))

    batch2 = date(2024, 1, 8)
    seed_silver(
        "customers",
        "customers",
        batch2,
        [
            _customer(1, loyalty_tier="gold"),  # changed -> should create a new version
            _customer(2),  # unchanged -> should stay as-is
            _customer(3),  # brand new customer
        ],
    )

    rows = build_dim_customer(batch2)

    # customer 1 now has two versions: one expired, one current
    customer_1_versions = sorted(
        (r for r in rows if r["customer_id"] == 1), key=lambda r: r["effective_date"]
    )
    assert len(customer_1_versions) == 2
    assert customer_1_versions[0]["is_current"] is False
    assert customer_1_versions[0]["loyalty_tier"] == "bronze"
    assert customer_1_versions[0]["end_date"] == date(2024, 1, 7)
    assert customer_1_versions[1]["is_current"] is True
    assert customer_1_versions[1]["loyalty_tier"] == "gold"
    assert customer_1_versions[1]["effective_date"] == batch2
    assert customer_1_versions[0]["customer_sk"] != customer_1_versions[1]["customer_sk"]

    # customer 2 unchanged -> exactly one row, still current, same surrogate key
    customer_2_versions = [r for r in rows if r["customer_id"] == 2]
    assert len(customer_2_versions) == 1
    assert customer_2_versions[0]["is_current"] is True

    # customer 3 is brand new -> one current row
    customer_3_versions = [r for r in rows if r["customer_id"] == 3]
    assert len(customer_3_versions) == 1
    assert customer_3_versions[0]["is_current"] is True
    assert customer_3_versions[0]["effective_date"] == batch2


@mock_aws
def test_no_incoming_data_returns_existing_gold_table_unchanged():
    ensure_bucket(LakeLayer.GOLD)
    batch1 = date(2024, 1, 1)
    seed_silver("customers", "customers", batch1, [_customer(1)])
    existing = build_dim_customer(batch1)
    write_gold_table("dim_customer", existing)

    # No Silver data landed for this batch_date (e.g. upstream skipped a run).
    result = build_dim_customer(date(2024, 1, 2))

    assert len(result) == 1
    assert result[0]["customer_id"] == 1
