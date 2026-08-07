"""Unit tests for the synthetic order generator's pure logic.

No Spark/Databricks dependency here - generate_order/generate_batch/
write_batch are plain Python, testable without a Spark session.
"""

import json
from pathlib import Path

from faker import Faker

from jobs.generate_synthetic_orders import (
    REGIONS,
    STATUSES,
    generate_batch,
    generate_order,
    write_batch,
)


class _FixedRandom:
    """Stand-in for random.Random with fully predictable outputs.

    `random()` returns values from a fixed sequence (one per call, in order)
    so tests can drive generate_order down a specific branch. `uniform(a, b)`
    always returns `a`, so the two possible amount ranges are distinguishable
    by value (5.0 for a normal amount, -50.0 for the invalid-amount branch).
    """

    def __init__(self, random_values):
        self._random_values = iter(random_values)

    def random(self):
        return next(self._random_values)

    def uniform(self, a, b):
        return a

    def choice(self, seq):
        return seq[0]

    def getrandbits(self, k):
        return 1


def test_generate_order_is_valid_when_not_flagged_invalid():
    order = generate_order(Faker(), _FixedRandom([0.99]))

    assert order["order_id"] is not None
    assert order["amount"] == 5.0
    assert order["status"] == STATUSES[0]
    assert order["region"] == REGIONS[0]
    assert isinstance(order["customer_id"], str)


def test_generate_order_can_produce_null_order_id():
    order = generate_order(Faker(), _FixedRandom([0.0, 0.0]))

    assert order["order_id"] is None
    assert order["amount"] == 5.0


def test_generate_order_can_produce_negative_amount():
    order = generate_order(Faker(), _FixedRandom([0.0, 0.99]))

    assert order["order_id"] is not None
    assert order["amount"] == -50.0


def test_generate_batch_returns_requested_size():
    orders = generate_batch(5, seed=42)

    expected_keys = {"order_id", "customer_id", "amount", "status", "region"}
    assert len(orders) == 5
    assert all(set(order) == expected_keys for order in orders)


def test_generate_batch_is_deterministic_with_a_seed():
    assert generate_batch(5, seed=42) == generate_batch(5, seed=42)


def test_write_batch_writes_newline_delimited_json(tmp_path):
    orders = generate_batch(3, seed=1)

    file_path = write_batch(orders, str(tmp_path))

    lines = Path(file_path).read_text().splitlines()
    assert [json.loads(line) for line in lines] == orders
    assert file_path.startswith(str(tmp_path))
    assert file_path.endswith(".json")
