"""One-time setup job: creates the Unity Catalog and LakeBase objects that
orders_etl_pipeline and sync_lakebase_job depend on but that nothing else
creates automatically.

Safe to re-run - every statement in sql/setup_unity_catalog.sql and
sql/setup_lakebase.sql is idempotent (``CREATE ... IF NOT EXISTS``).
Intended to be run on demand (``databricks bundle run setup_resources_job``),
not on a schedule.
"""

import logging
import sys
from pathlib import Path

# Databricks runs spark_python_task files with only the file's own directory
# on sys.path, not the repo's src/ root that pytest uses (via setup.cfg's
# `pythonpath = src`). Insert src/ explicitly so `jobs.sync_lakebase` resolves
# the same way in both places.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.sync_lakebase import (  # noqa: E402
    build_spark_session,
    connect_to_lakebase,
    load_lakebase_connection,
)
from pyspark.sql import SparkSession  # noqa: E402

try:
    from pyspark.dbutils import DBUtils
except ImportError:  # pragma: no cover - not available off-platform
    DBUtils = None

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"
UNITY_CATALOG_DDL = SQL_DIR / "setup_unity_catalog.sql"
LAKEBASE_DDL = SQL_DIR / "setup_lakebase.sql"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("setup_resources")


def read_statements(sql_path: Path) -> list[str]:
    """Split a .sql file into individual, non-empty statements on ';'.

    This is a naive split with no awareness of quoted strings - a ';' inside
    a string literal (e.g. a COMMENT value) will incorrectly split one
    statement into two. Keep semicolons out of string literals in the .sql
    files under sql/ rather than making this parser quote-aware.
    """
    text = sql_path.read_text()
    return [statement.strip() for statement in text.split(";") if statement.strip()]


def apply_unity_catalog_ddl(spark: SparkSession) -> None:
    """Run each statement in sql/setup_unity_catalog.sql via Spark SQL."""
    for statement in read_statements(UNITY_CATALOG_DDL):
        logger.info("Unity Catalog DDL: %s", " ".join(statement.split())[:100])
        spark.sql(statement)


def apply_lakebase_ddl(dbutils) -> None:
    """Run each statement in sql/setup_lakebase.sql against LakeBase."""
    conn = load_lakebase_connection(dbutils)
    pg_conn = connect_to_lakebase(conn)
    try:
        with pg_conn:
            with pg_conn.cursor() as cursor:
                for statement in read_statements(LAKEBASE_DDL):
                    logger.info("LakeBase DDL: %s", " ".join(statement.split())[:100])
                    cursor.execute(statement)
    finally:
        pg_conn.close()


def run() -> None:
    """Apply the Unity Catalog and LakeBase setup DDL."""
    spark = build_spark_session("setup_resources")
    dbutils = DBUtils(spark) if DBUtils is not None else None

    apply_unity_catalog_ddl(spark)
    apply_lakebase_ddl(dbutils)
    logger.info("Setup complete.")


if __name__ == "__main__":
    try:
        run()
    except Exception:
        logger.exception("Setup failed.")
        sys.exit(1)
