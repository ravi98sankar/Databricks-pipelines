"""Lakeflow Declarative Pipeline (Delta Live Tables) for the Orders ETL/ELT flow.

Implements a medallion architecture:
    Bronze  -> raw incremental ingestion via Auto Loader
    Silver  -> validated, deduplicated, standardized order records
    Gold    -> daily customer spend metrics for downstream consumption
"""

import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RAW_ORDERS_PATH = "/Volumes/main/orders_raw/landing/orders"
BRONZE_TABLE_COMMENT = "Raw orders ingested incrementally from cloud storage via Auto Loader."
SILVER_TABLE_COMMENT = "Cleansed, deduplicated, and validated orders."
GOLD_TABLE_COMMENT = (
    "Daily customer-level spend and activity metrics "
    "(stores composable components for incremental refresh)."
)
GOLD_ANALYTICS_COMMENT = "Derived analytics with average order value and variability measures."

TEXT_COLUMNS_TO_STANDARDIZE = ["customer_id", "status", "region"]


# ---------------------------------------------------------------------------
# Bronze Layer
# ---------------------------------------------------------------------------

@dlt.table(
    name="bronze_orders",
    comment=BRONZE_TABLE_COMMENT,
    table_properties={"quality": "bronze"},
)
def bronze_orders() -> DataFrame:
    """Ingest raw order JSON files incrementally using Auto Loader.

    Schema evolution is set to ``addNewColumns`` so new fields introduced by
    upstream producers are automatically incorporated into the table schema
    rather than failing the stream. The rescued data column captures any
    values that don't conform to the inferred/expected schema so no data is
    silently dropped.
    """
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821 - injected by the Lakeflow/DLT runtime
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(RAW_ORDERS_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ---------------------------------------------------------------------------
# Silver Layer
# ---------------------------------------------------------------------------

def _standardize_text_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    """Trim whitespace and normalize casing for the given text columns.

    Args:
        df: Input DataFrame.
        columns: Names of the string columns to standardize.

    Returns:
        DataFrame with each listed column trimmed and upper-cased.
    """
    for column in columns:
        df = df.withColumn(column, F.upper(F.trim(F.col(column))))
    return df


def _deduplicate_orders(df: DataFrame) -> DataFrame:
    """Deduplicate streaming order records on ``order_id``.

    A watermark on ``_ingested_at`` bounds the state Spark must retain for
    stateful deduplication, keeping the operation feasible for a continuously
    running stream.

    Args:
        df: Streaming DataFrame containing an ``order_id`` and
            ``_ingested_at`` column.

    Returns:
        DataFrame with duplicate ``order_id`` records removed.
    """
    return df.withWatermark("_ingested_at", "1 hour").dropDuplicates(["order_id"])


@dlt.table(
    name="silver_orders",
    comment=SILVER_TABLE_COMMENT,
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
def silver_orders() -> DataFrame:
    """Validate, standardize, and deduplicate bronze order records.

    Reads the bronze table as a stateful stream, drops records that fail
    quality expectations (missing ``order_id`` or non-positive ``amount``),
    standardizes text columns, stamps each row with a processing timestamp,
    and deduplicates on ``order_id``.
    """
    df = dlt.read_stream("bronze_orders")
    df = _standardize_text_columns(df, TEXT_COLUMNS_TO_STANDARDIZE)
    df = df.withColumn("_processed_at", F.current_timestamp())
    df = _deduplicate_orders(df)
    return df


# ---------------------------------------------------------------------------
# Gold Layer
# ---------------------------------------------------------------------------

@dlt.table(
    name="gold_daily_customer_metrics",
    comment=GOLD_TABLE_COMMENT,
    table_properties={"quality": "gold"},
)
def gold_daily_customer_metrics() -> DataFrame:
    """Aggregate daily customer spend and activity metrics from silver orders.

    Computes, per customer per order date, the total amount spent, the
    number of orders placed, and the most recent activity timestamp.
    """
    df = dlt.read("silver_orders")
    return (
        df.withColumn("order_date", F.to_date("_processed_at"))
        .groupBy("customer_id", "order_date")
        .agg(
            F.sum("amount").alias("total_spend"),
            F.count("order_id").alias("total_orders"),
            F.sum(F.col("amount") * F.col("amount")).alias("sum_of_squares"),
            F.max("_processed_at").alias("last_active_time"),
        )
    )


@dlt.table(
    name="gold_daily_customer_analytics",
    comment=GOLD_ANALYTICS_COMMENT,
    table_properties={"quality": "gold"},
)
def gold_daily_customer_analytics() -> DataFrame:
    """Derived analytics with statistical measures.

    Calculates average order value, variance, and standard deviation.
    """
    return (
        dlt.read("gold_daily_customer_metrics")
        .withColumn("avg_order_value", F.col("total_spend") / F.col("total_orders"))
        .withColumn(
            "variance",
            # Clamped to 0: the E[X^2] - E[X]^2 formula can go slightly
            # negative under floating-point rounding, which would otherwise
            # make sqrt() below silently return null instead of ~0.
            F.greatest(
                (F.col("sum_of_squares") / F.col("total_orders"))
                - F.pow(F.col("total_spend") / F.col("total_orders"), 2),
                F.lit(0.0),
            ),
        )
        .withColumn("stddev", F.sqrt(F.col("variance")))
        .select(
            "customer_id",
            "order_date",
            "total_spend",
            "total_orders",
            "avg_order_value",
            "stddev",
            "last_active_time",
        )
    )
