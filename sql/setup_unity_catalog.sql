-- Idempotent setup for the Unity Catalog objects orders_etl_pipeline needs
-- but that nothing auto-creates: the landing schema and volume that Auto
-- Loader reads from (RAW_ORDERS_PATH in src/pipelines/orders_etl.py).
--
-- The pipeline's own target schema (orders_dev / orders) is NOT created
-- here - Lakeflow Declarative Pipelines create their target schema
-- automatically on first run if the deploying principal has CREATE SCHEMA
-- on the catalog.
--
-- Applied by src/jobs/setup_resources.py via the setup_resources job.

CREATE SCHEMA IF NOT EXISTS main.orders_raw
    COMMENT 'Landing zone for raw incoming order files consumed by Auto Loader.';

CREATE VOLUME IF NOT EXISTS main.orders_raw.landing
    COMMENT 'Auto Loader landing volume, read by orders_etl_pipeline from landing/orders/.';
