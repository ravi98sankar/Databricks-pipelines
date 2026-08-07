"""Generate synthetic orders and land them for orders_etl_pipeline to ingest.

Writes one newline-delimited JSON file per run into the Auto Loader landing
volume (``RAW_ORDERS_PATH`` in ``pipelines/orders_etl.py``), simulating a
trickle of incoming order files. Databricks Jobs don't support long-running
daemon processes, so "background" generation here means running this script
on a short schedule (see ``generate_synthetic_orders_job`` in
databricks.yml) rather than looping forever in a single job run.

A small fraction of records are intentionally invalid (missing ``order_id``
or a non-positive ``amount``) so silver_orders' ``@dlt.expect_or_drop``
expectations in orders_etl.py have real bad rows to drop.
"""

import json
import logging
import os
import random
import sys
import uuid
from pathlib import Path
from typing import Optional

from faker import Faker

# Defaults to the real Unity Catalog landing volume; override for local/manual
# runs with e.g. `LANDING_PATH=./local_data/orders python generate_synthetic_orders.py`.
# Deliberately not auto-detected: there's no environment signal (env var or
# mount point) confirmed to reliably distinguish "real Databricks serverless
# job" from "developer's laptop" - both a DATABRICKS_RUNTIME_VERSION check and
# a `/Volumes` mount check were tried and rejected here, the latter because
# `/Volumes` is a standard macOS mount point that exists on every Mac. An
# explicit env var with a safe production default can't misfire either way.
LANDING_PATH = os.environ.get("LANDING_PATH", "/Volumes/main/orders_raw/landing/orders")

BATCH_SIZE = 50
INVALID_RECORD_RATE = 0.05

STATUSES = ["pending", "shipped", "delivered", "cancelled"]
REGIONS = ["us-east", "us-west", "eu-west", "apac"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("generate_synthetic_orders")


def generate_order(fake: Faker, rng: random.Random) -> dict:
    """Build a single synthetic order record.

    ``rng`` decides whether this record is intentionally invalid; ``fake``
    supplies the customer identifier. Both are passed in explicitly so tests
    can seed them for deterministic output. order_id is derived from ``rng``
    rather than ``uuid.uuid4()`` for the same reason - uuid4 draws from OS
    randomness and would ignore any seed.
    """
    order_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
    amount = round(rng.uniform(5.0, 500.0), 2)

    if rng.random() < INVALID_RECORD_RATE:
        if rng.random() < 0.5:
            order_id = None
        else:
            amount = round(rng.uniform(-50.0, 0.0), 2)

    return {
        "order_id": order_id,
        "customer_id": fake.uuid4(),
        "amount": amount,
        "status": rng.choice(STATUSES),
        "region": rng.choice(REGIONS),
    }


def generate_batch(size: int, seed: Optional[int] = None) -> list[dict]:
    """Generate a batch of synthetic order records."""
    if seed is not None:
        Faker.seed(seed)
    fake = Faker()
    rng = random.Random(seed)
    return [generate_order(fake, rng) for _ in range(size)]


def write_batch(orders: list[dict], landing_path: str) -> str:
    """Write a batch of orders as newline-delimited JSON to the landing path.

    Returns the path of the file written.
    """
    Path(landing_path).mkdir(parents=True, exist_ok=True)
    file_path = Path(landing_path) / f"orders_{uuid.uuid4().hex}.json"
    with open(str(file_path), "w") as f:
        for order in orders:
            f.write(json.dumps(order) + "\n")
    return str(file_path)


def run() -> None:
    """Generate one batch of synthetic orders and land it as a new file."""
    orders = generate_batch(BATCH_SIZE)
    file_path = write_batch(orders, LANDING_PATH)
    logger.info("Wrote %d synthetic orders to %s", len(orders), file_path)


if __name__ == "__main__":
    try:
        run()
    except Exception:
        logger.exception("Synthetic order generation failed.")
        sys.exit(1)
