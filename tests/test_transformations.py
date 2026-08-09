"""Unit tests for the pure transformation helpers in the pipeline modules.

These target the plain functions that don't require a running Databricks
workspace (no ``dlt.table``-decorated entry points, no live Postgres/JDBC
connection), so they run locally and in CI against a local Spark session.
"""

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

import jobs.sync_lakebase as sync_lakebase_module
from jobs.sync_lakebase import (
    DEFAULT_CATALOG,
    DEFAULT_SCHEMA,
    generate_lakebase_token,
    get_secret,
    load_lakebase_connection,
    parse_args,
)
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


def test_get_secret_falls_back_to_env_var(monkeypatch):
    monkeypatch.setenv("JDBC_HOST", "env-host")

    assert get_secret(None, scope="lakebase", key="jdbc_host") == "env-host"


def test_get_secret_raises_when_unresolved(monkeypatch):
    monkeypatch.delenv("MISSING_KEY", raising=False)

    with pytest.raises(RuntimeError):
        get_secret(None, scope="lakebase", key="missing_key")


class _FakeCredential:
    def __init__(self, token):
        self.token = token


class _FakeDatabaseAPI:
    def __init__(self, token):
        self._token = token
        self.calls = []

    def generate_database_credential(self, request_id, instance_names):
        self.calls.append({"request_id": request_id, "instance_names": instance_names})
        return _FakeCredential(self._token)


class _FakeWorkspaceClient:
    def __init__(self, token="fake-token"):
        self.database = _FakeDatabaseAPI(token)


def test_generate_lakebase_token_requests_the_given_instance(monkeypatch):
    fake_client = _FakeWorkspaceClient(token="minted-token")
    monkeypatch.setattr(sync_lakebase_module, "WorkspaceClient", lambda: fake_client)

    token = generate_lakebase_token("my-instance")

    assert token == "minted-token"
    assert fake_client.database.calls[0]["instance_names"] == ["my-instance"]


def test_load_lakebase_connection_mints_token_instead_of_reading_a_password(monkeypatch):
    monkeypatch.setenv("JDBC_HOST", "db.example.com")
    monkeypatch.setenv("JDBC_PORT", "5432")
    monkeypatch.setenv("JDBC_DATABASE", "app")
    monkeypatch.setenv("JDBC_USERNAME", "reader")
    monkeypatch.setenv("INSTANCE_NAME", "my-instance")
    fake_client = _FakeWorkspaceClient(token="minted-token")
    monkeypatch.setattr(sync_lakebase_module, "WorkspaceClient", lambda: fake_client)

    conn = load_lakebase_connection(None)

    assert conn.host == "db.example.com"
    assert conn.port == "5432"
    assert conn.database == "app"
    assert conn.username == "reader"
    assert conn.password == "minted-token"


def test_parse_args_defaults_when_run_standalone():
    args = parse_args([])

    assert args.catalog == DEFAULT_CATALOG
    assert args.schema == DEFAULT_SCHEMA


def test_parse_args_uses_job_supplied_catalog_and_schema():
    args = parse_args(["--catalog", "main", "--schema", "orders_dev"])

    assert args.catalog == "main"
    assert args.schema == "orders_dev"
