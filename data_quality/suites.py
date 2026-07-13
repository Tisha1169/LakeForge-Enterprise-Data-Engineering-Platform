"""Expectation suites, one builder function per table. Each function
constructs (and registers on the context) a fresh ExpectationSuite —
referential-integrity suites take the valid-ID set as a parameter since it's
only known at validation time, not suite-definition time.
"""

from __future__ import annotations

import great_expectations as gx
from great_expectations.data_context import AbstractDataContext

LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]
ORDER_STATUSES = ["pending", "completed", "cancelled", "refunded", "unknown"]


def _new_suite(context: AbstractDataContext, name: str) -> gx.ExpectationSuite:
    """Suites are rebuilt fresh on every validation run — add_or_update
    overwrites any suite left over from a previous run rather than
    accumulating stale expectations."""
    return context.suites.add_or_update(gx.ExpectationSuite(name=name))


def build_bronze_customers_suite(context: AbstractDataContext) -> gx.ExpectationSuite:
    """Bronze is intentionally schema-on-read (Phase 8) — light validation
    only: the columns we expect to exist are present. No null/uniqueness
    checks here, since Bronze deliberately preserves raw, unvalidated data."""
    suite = _new_suite(context, "bronze_customers_suite")
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
                "customer_id",
                "email",
                "first_name",
                "last_name",
                "signup_date",
                "loyalty_tier",
                "phone",
                "_ingested_at",
                "_source_file",
                "_batch_date",
            ],
            exact_match=False,
        )
    )
    return suite


def build_silver_customers_suite(context: AbstractDataContext) -> gx.ExpectationSuite:
    suite = _new_suite(context, "silver_customers_suite")
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="customer_id"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="loyalty_tier", value_set=LOYALTY_TIERS, mostly=0.95
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="email", regex=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", mostly=0.9
        )
    )
    return suite


def build_silver_orders_suite(context: AbstractDataContext) -> gx.ExpectationSuite:
    suite = _new_suite(context, "silver_orders_suite")
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="order_status", value_set=ORDER_STATUSES)
    )
    return suite


def build_silver_order_lines_suite(
    context: AbstractDataContext, valid_order_ids: list[int]
) -> gx.ExpectationSuite:
    """Referential integrity: every order_id here must exist in the Silver
    orders table for the same batch. valid_order_ids is computed by the
    caller (great_expectations/runner.py) from a fresh read of Silver
    orders — this suite can't know that set at definition time."""
    suite = _new_suite(context, "silver_order_lines_suite")
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_line_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="order_line_id"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="order_id", value_set=valid_order_ids)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="quantity", min_value=1, strict_min=False
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="unit_price", min_value=0, strict_min=False
        )
    )
    return suite


def build_silver_products_suite(context: AbstractDataContext) -> gx.ExpectationSuite:
    suite = _new_suite(context, "silver_products_suite")
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="product_id"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="unit_price", min_value=0, strict_min=False
        )
    )
    return suite


def build_silver_stores_suite(context: AbstractDataContext) -> gx.ExpectationSuite:
    suite = _new_suite(context, "silver_stores_suite")
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="store_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="store_id"))
    return suite
