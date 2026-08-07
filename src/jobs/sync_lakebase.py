"""Sync the curated Gold table to Databricks LakeBase (Serverless Postgres).

Reads ``gold_daily_customer_metrics`` from Unity Catalog, stages it into a
Postgres staging table over JDBC, then performs an ``INSERT ... ON CONFLICT``
upsert from staging into the target LakeBase table. Intended to run as a
scheduled Databricks Job task.
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

try:
    from pyspark.dbutils import DBUtils
except ImportError:  # pragma: no cover - not available off-platform
    DBUtils = None

try:
    import psycopg2
    from psycopg2.extensions import connection as PGConnection
except ImportError:  # pragma: no cover - installed via cluster library
    psycopg2 = None
    PGConnection = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Catalog/schema default to the values used when this script is run
# standalone (no --catalog/--schema args). The deployed job always passes
# both explicitly via job parameters templated from the bundle's catalog/
# schema variables (see databricks.yml) - the schema differs per target
# (orders_dev vs orders), so it can't be a fixed module-level constant.
DEFAULT_CATALOG = "main"
DEFAULT_SCHEMA = "orders"
GOLD_TABLE_NAME = "gold_daily_customer_metrics"
STAGING_TABLE = "staging_daily_customer_metrics"
TARGET_TABLE = "daily_customer_metrics"
KEY_COLUMNS = ["customer_id", "order_date"]
UPDATE_COLUMNS = ["total_spend", "total_orders", "last_active_time"]

SECRET_SCOPE = "lakebase"
JDBC_HOST_KEY = "jdbc_host"
JDBC_PORT_KEY = "jdbc_port"
JDBC_DATABASE_KEY = "jdbc_database"
JDBC_USER_KEY = "jdbc_username"
JDBC_PASSWORD_KEY = "jdbc_password"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("sync_lakebase")


@dataclass
class LakebaseConnection:
    """Connection details for the target LakeBase Postgres instance."""

    host: str
    port: str
    database: str
    username: str
    password: str

    @property
    def jdbc_url(self) -> str:
        """Build the JDBC connection URL for Spark's JDBC writer."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def connection_properties(self) -> dict[str, str]:
        """Connection properties passed to Spark's JDBC writer."""
        return {
            "user": self.username,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }


# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------

def get_secret(dbutils, scope: str, key: str) -> str:
    """Retrieve a secret from Databricks Secrets, falling back to an env var.

    Args:
        dbutils: An initialized ``DBUtils`` instance, or ``None`` if
            unavailable in the current runtime.
        scope: The Databricks secret scope name.
        key: The secret key within the scope. Also used (upper-cased) as the
            environment variable name for the fallback lookup.

    Returns:
        The resolved secret value.

    Raises:
        RuntimeError: If the secret cannot be resolved from either source.
    """
    if dbutils is not None:
        try:
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception as exc:  # noqa: BLE001 - surface as a clear runtime error below
            logger.warning("Could not read secret %s/%s from dbutils: %s", scope, key, exc)

    env_value = os.environ.get(key.upper())
    if env_value:
        return env_value

    raise RuntimeError(f"Unable to resolve secret '{key}' from scope '{scope}' or env var.")


def load_lakebase_connection(dbutils) -> LakebaseConnection:
    """Assemble LakeBase connection details from Databricks Secrets/env vars."""
    return LakebaseConnection(
        host=get_secret(dbutils, SECRET_SCOPE, JDBC_HOST_KEY),
        port=get_secret(dbutils, SECRET_SCOPE, JDBC_PORT_KEY),
        database=get_secret(dbutils, SECRET_SCOPE, JDBC_DATABASE_KEY),
        username=get_secret(dbutils, SECRET_SCOPE, JDBC_USER_KEY),
        password=get_secret(dbutils, SECRET_SCOPE, JDBC_PASSWORD_KEY),
    )


# ---------------------------------------------------------------------------
# Spark / Delta
# ---------------------------------------------------------------------------

def build_spark_session(app_name: str = "sync_lakebase") -> SparkSession:
    """Initialize (or fetch) a Spark session configured with Delta extensions."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_gold_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read the curated Gold metrics table from Unity Catalog.

    Args:
        spark: Active SparkSession.
        table_name: Fully qualified Unity Catalog table name.

    Returns:
        DataFrame containing the Gold table contents.
    """
    logger.info("Reading gold table: %s", table_name)
    return spark.read.table(table_name)


# ---------------------------------------------------------------------------
# LakeBase sync
# ---------------------------------------------------------------------------

def connect_to_lakebase(conn: LakebaseConnection) -> "PGConnection":
    """Open a psycopg2 connection to the LakeBase Postgres instance.

    Raises:
        RuntimeError: If psycopg2 is not available in the current environment.
    """
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 is required to connect to LakeBase; install it on the job cluster."
        )
    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.database,
        user=conn.username,
        password=conn.password,
    )


def write_to_staging(df: DataFrame, conn: LakebaseConnection, staging_table: str) -> None:
    """Overwrite the Postgres staging table with the latest Gold snapshot.

    Args:
        df: DataFrame to stage.
        conn: LakeBase connection details.
        staging_table: Name of the staging table in Postgres.
    """
    logger.info("Writing %d rows to staging table '%s'", df.count(), staging_table)
    (
        df.write.format("jdbc")
        .option("url", conn.jdbc_url)
        .option("dbtable", staging_table)
        .option("user", conn.connection_properties["user"])
        .option("password", conn.connection_properties["password"])
        .option("driver", conn.connection_properties["driver"])
        .mode("overwrite")
        .save()
    )


def upsert_from_staging(
    conn: LakebaseConnection,
    staging_table: str,
    target_table: str,
    key_columns: list[str],
    update_columns: list[str],
) -> None:
    """Upsert staged rows into the target LakeBase table.

    Executes an ``INSERT ... ON CONFLICT (key_columns) DO UPDATE`` statement
    so the target table always reflects the latest staged metrics without
    duplicating rows on the conflict keys.

    Args:
        conn: LakeBase connection details.
        staging_table: Source staging table already populated via JDBC write.
        target_table: Destination table to upsert into.
        key_columns: Columns forming the conflict/uniqueness key.
        update_columns: Non-key columns to overwrite on conflict.

    Raises:
        RuntimeError: If psycopg2 is not available in the current environment.
    """
    all_columns = key_columns + update_columns
    columns_sql = ", ".join(all_columns)
    conflict_sql = ", ".join(key_columns)
    update_sql = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)

    upsert_statement = f"""
        INSERT INTO {target_table} ({columns_sql})
        SELECT {columns_sql} FROM {staging_table}
        ON CONFLICT ({conflict_sql})
        DO UPDATE SET {update_sql}
    """

    logger.info("Upserting staging table '%s' into target '%s'", staging_table, target_table)
    pg_conn = connect_to_lakebase(conn)
    try:
        with pg_conn:
            with pg_conn.cursor() as cursor:
                cursor.execute(upsert_statement)
                logger.info("Upsert complete: %d rows affected", cursor.rowcount)
    finally:
        pg_conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse --catalog/--schema, defaulting to standalone-run values."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    return parser.parse_args(argv)


def run() -> None:
    """Execute the full Gold -> LakeBase sync workflow."""
    args = parse_args()
    gold_table = f"{args.catalog}.{args.schema}.{GOLD_TABLE_NAME}"

    spark = build_spark_session()
    dbutils = DBUtils(spark) if DBUtils is not None else None

    try:
        conn = load_lakebase_connection(dbutils)
        gold_df = read_gold_table(spark, gold_table)
        write_to_staging(gold_df, conn, STAGING_TABLE)
        upsert_from_staging(conn, STAGING_TABLE, TARGET_TABLE, KEY_COLUMNS, UPDATE_COLUMNS)
        logger.info("LakeBase sync completed successfully.")
    except Exception:
        logger.exception("LakeBase sync failed.")
        raise


if __name__ == "__main__":
    try:
        run()
    except Exception:
        sys.exit(1)
