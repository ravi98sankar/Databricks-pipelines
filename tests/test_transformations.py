"""Unit tests for the pure transformation helpers in the pipeline modules.

These target the plain functions that don't require a running Databricks
workspace (no ``dlt.table``-decorated entry points, no live Postgres/JDBC
connection), so they run locally and in CI against a local Spark session.
"""

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from jobs.sync_lakebase import LakebaseConnection, get_secret
from pipelines.orders_etl import _deduplicate_orders, _standardize_text_columns


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_transformations")
        .getOrCreate()
    )
    yield session
    session.stop()


# TODO: re-enable once local Spark session creation works in CI. The pyspark
# build pulled in here has Databricks-internal patches (via databricks-dlt's
# dependency chain) that reject SparkSession.builder.getOrCreate() outside of
# Databricks Connect - see the "Only remote Spark sessions using Databricks
# Connect are supported" RuntimeError. Doesn't block real deploys: the actual
# pipeline/job always runs against an already-active cluster session, never a
# fresh local one.
@pytest.mark.skip(reason="local SparkSession creation is blocked in CI - see TODO above")
def test_standardize_text_columns_trims_and_upper_cases(spark):
    df = spark.createDataFrame(
        [Row(customer_id=" cust-1 ", status="active", region=" us-east ")]
    )

    result = _standardize_text_columns(df, ["customer_id", "status", "region"])

    row = result.collect()[0]
    assert row.customer_id == "CUST-1"
    assert row.status == "ACTIVE"
    assert row.region == "US-EAST"


@pytest.mark.skip(reason="local SparkSession creation is blocked in CI - see TODO above")
def test_deduplicate_orders_drops_duplicate_order_ids(spark):
    df = spark.createDataFrame(
        [
            Row(order_id="o1", amount=10.0, _ingested_at="2026-01-01T00:00:00"),
            Row(order_id="o1", amount=10.0, _ingested_at="2026-01-01T00:00:01"),
            Row(order_id="o2", amount=20.0, _ingested_at="2026-01-01T00:00:02"),
        ]
    ).withColumn("_ingested_at", F.to_timestamp("_ingested_at"))

    result = _deduplicate_orders(df)

    order_ids = sorted(row.order_id for row in result.collect())
    assert order_ids == ["o1", "o2"]


def test_lakebase_connection_jdbc_url():
    conn = LakebaseConnection(
        host="db.example.com",
        port="5432",
        database="app",
        username="reader",
        password="secret",
    )

    assert conn.jdbc_url == "jdbc:postgresql://db.example.com:5432/app"


def test_lakebase_connection_properties():
    conn = LakebaseConnection(
        host="db.example.com",
        port="5432",
        database="app",
        username="reader",
        password="secret",
    )

    assert conn.connection_properties == {
        "user": "reader",
        "password": "secret",
        "driver": "org.postgresql.Driver",
    }


def test_get_secret_falls_back_to_env_var(monkeypatch):
    monkeypatch.setenv("JDBC_HOST", "env-host")

    assert get_secret(None, scope="lakebase", key="jdbc_host") == "env-host"


def test_get_secret_raises_when_unresolved(monkeypatch):
    monkeypatch.delenv("MISSING_KEY", raising=False)

    with pytest.raises(RuntimeError):
        get_secret(None, scope="lakebase", key="missing_key")
