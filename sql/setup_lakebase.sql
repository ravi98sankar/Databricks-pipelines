-- Idempotent setup for the LakeBase (Postgres) target table that
-- sync_lakebase.py upserts into. The PRIMARY KEY on (customer_id,
-- order_date) is required for the "ON CONFLICT (customer_id, order_date)"
-- clause in upsert_from_staging() - without it, Postgres rejects the upsert
-- with "there is no unique or exclusion constraint matching the ON CONFLICT
-- specification".
--
-- Applied by src/jobs/setup_resources.py via the setup_resources job.

CREATE TABLE IF NOT EXISTS daily_customer_metrics (
    customer_id      TEXT NOT NULL,
    order_date       DATE NOT NULL,
    total_spend      NUMERIC NOT NULL,
    total_orders     BIGINT NOT NULL,
    last_active_time TIMESTAMP NOT NULL,
    PRIMARY KEY (customer_id, order_date)
);
