"""Pytest configuration for the local test suite.

``databricks-dlt`` is a stub package: importing anything decorated with
``@dlt.table``/``@dlt.expect_or_drop`` raises by default, since Lakeflow
Declarative Pipelines aren't meant to run outside a workspace. Calling
``enable_local_execution()`` here - before any test module imports
``pipelines.orders_etl`` - downgrades that to a warning so the module can be
imported for unit-testing its plain helper functions.
"""

import dlt

dlt.enable_local_execution()
