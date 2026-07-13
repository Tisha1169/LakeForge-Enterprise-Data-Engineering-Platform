"""Runs an expectation suite against a Bronze/Silver table, reading through
our already-tested boto3 boundary (pipelines.bronze/pipelines.silver) rather
than pointing GX at S3 directly — consistent with the rest of the platform's
I/O pattern (see spark/README.md for the same reasoning applied to Spark).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date

import great_expectations as gx
import pandas as pd
from monitoring.logging_config import get_logger
from pipelines.bronze.reader import read_bronze
from pipelines.silver.reader import read_silver

from data_quality.context import get_context
from data_quality.suites import (
    build_bronze_customers_suite,
    build_silver_customers_suite,
    build_silver_order_lines_suite,
    build_silver_orders_suite,
    build_silver_products_suite,
    build_silver_stores_suite,
)

logger = get_logger(__name__)


@dataclass
class QualityResult:
    suite_name: str
    success: bool
    evaluated_expectations: int
    successful_expectations: int
    row_count: int


def _validate_dataframe(context, suite: gx.ExpectationSuite, df: pd.DataFrame) -> QualityResult:
    unique_suffix = uuid.uuid4().hex[:8]
    data_source = context.data_sources.add_or_update_pandas(f"source_{suite.name}_{unique_suffix}")
    data_asset = data_source.add_dataframe_asset(name=f"asset_{unique_suffix}")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"batch_{unique_suffix}")

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=f"validation_{unique_suffix}", data=batch_definition, suite=suite
        )
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=f"checkpoint_{unique_suffix}",
            validation_definitions=[validation_definition],
            actions=[gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")],
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})
    validation_result = result.run_results[list(result.run_results.keys())[0]]

    stats = validation_result.statistics
    quality_result = QualityResult(
        suite_name=suite.name,
        success=bool(result.success),
        evaluated_expectations=stats["evaluated_expectations"],
        successful_expectations=stats["successful_expectations"],
        row_count=len(df),
    )
    logger.info(
        "quality.validated" if quality_result.success else "quality.failed",
        extra={"context": vars(quality_result)},
    )
    return quality_result


def validate_bronze_customers(batch_date: date) -> QualityResult:
    context = get_context()
    suite = build_bronze_customers_suite(context)
    df = pd.DataFrame(read_bronze("customers", "customers", batch_date))
    return _validate_dataframe(context, suite, df)


def validate_silver_customers(batch_date: date) -> QualityResult:
    context = get_context()
    suite = build_silver_customers_suite(context)
    df = pd.DataFrame(read_silver("customers", "customers", batch_date))
    return _validate_dataframe(context, suite, df)


def validate_silver_orders(batch_date: date) -> QualityResult:
    context = get_context()
    suite = build_silver_orders_suite(context)
    df = pd.DataFrame(read_silver("sales", "orders", batch_date))
    return _validate_dataframe(context, suite, df)


def validate_silver_order_lines(batch_date: date) -> QualityResult:
    """Referential integrity against Silver orders for the same batch_date
    — the valid order_id set is computed fresh here, not baked into a
    static suite (see suites.build_silver_order_lines_suite)."""
    context = get_context()
    orders = read_silver("sales", "orders", batch_date)
    valid_order_ids = [row["order_id"] for row in orders]
    suite = build_silver_order_lines_suite(context, valid_order_ids)
    df = pd.DataFrame(read_silver("sales_order_lines", "order_lines", batch_date))
    return _validate_dataframe(context, suite, df)


def validate_silver_products(batch_date: date) -> QualityResult:
    context = get_context()
    suite = build_silver_products_suite(context)
    df = pd.DataFrame(read_silver("sales_products", "products", batch_date))
    return _validate_dataframe(context, suite, df)


def validate_silver_stores(batch_date: date) -> QualityResult:
    context = get_context()
    suite = build_silver_stores_suite(context)
    df = pd.DataFrame(read_silver("sales_stores", "stores", batch_date))
    return _validate_dataframe(context, suite, df)
