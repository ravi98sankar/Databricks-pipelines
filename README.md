# Databricks Pipelines

Medallion-architecture orders pipeline built with **Databricks Asset Bundles (DABs)**:

- `src/pipelines/orders_etl.py` - Lakeflow Declarative Pipeline (DLT). Bronze (Auto Loader
  ingestion) -> Silver (validated/deduplicated orders) -> Gold (daily customer spend metrics).
- `src/jobs/sync_lakebase.py` - Scheduled job that reads the Gold table from Unity Catalog and
  upserts it into a LakeBase (serverless Postgres) table, via Databricks' native "postgresql"
  data source for the staging write and psycopg2 for the upsert.
- `src/jobs/setup_resources.py` - One-time, idempotent setup job: creates the Unity Catalog
  landing schema/volume and the LakeBase target table (applies `sql/*.sql`).
- `src/jobs/generate_synthetic_orders.py` - Lands a batch of fake orders every 5 minutes so
  `orders_etl_pipeline`'s Auto Loader has something to ingest. Deploys paused everywhere.
- `sql/setup_unity_catalog.sql` / `sql/setup_lakebase.sql` - The DDL `setup_resources_job` applies.
- `databricks.yml` - The bundle definition: `dev` and `prod` targets, and all four resources above.
- `.github/workflows/ci-cd.yml` - Lints, tests, validates the bundle, and deploys it (`dev`
  automatically, `prod` behind a required approval).

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.10+ | Local dev, tests, linting |
| Java (JDK) | 11 or 17 | Required by PySpark to run tests locally |
| [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) | 0.220+ | `databricks bundle` commands |
| VS Code + [Databricks extension](https://marketplace.visualstudio.com/items?itemName=databricks.databricks) | latest | Bundle-aware editing, run/debug on a cluster |
| Access to a Databricks workspace | - | Unity Catalog `main` catalog, permission to create pipelines/jobs |

## 1. Clone and set up a virtual environment

```bash
git clone https://github.com/ravi98sankar/Databricks-pipelines.git
cd Databricks-pipelines
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

`requirements.txt` installs `pyspark`, `databricks-dlt` (the local unit-testing stub for `dlt`),
`psycopg2-binary`, `Faker`, `pytest`, and `flake8` — everything needed to lint and test the code
without a live workspace. **Do not** add `databricks-connect` to this same environment; it conflicts with
plain `pyspark`. If you want to run/debug code against a real cluster from VS Code, create a
*separate* venv with `databricks-connect` instead (see [Section 4](#4-run--debug-from-vs-code)).

## 2. Authenticate the Databricks CLI

Pick one:

**Option A - CLI profile (recommended for local dev):**

```bash
databricks auth login --host https://<your-workspace-host>
```

This writes a profile to `~/.databrickscfg`. Reference it with `--profile <name>` on bundle
commands, or set `DATABRICKS_CONFIG_PROFILE=<name>`.

**Option B - environment variables (used by CI, also works locally):**

```bash
export DATABRICKS_HOST="https://<your-workspace-host>"
export DATABRICKS_TOKEN="<personal-access-token>"
```

`databricks.yml` intentionally omits `workspace.host` per target so both targets resolve the
workspace from whichever of these you have active — set the right host/token before switching
between `dev` and `prod`, or add an explicit `workspace.host` to a target if they're always
different workspaces.

## 3. Validate and deploy the bundle

```bash
# Check the bundle compiles and resources are well-formed (defaults to the `dev` target)
databricks bundle validate

# Deploy everything to your dev workspace
databricks bundle deploy -t dev
```

`bundle deploy` uploads the source files under `src/` and creates/updates four resources:

- **`orders_etl_<target>`** - the Lakeflow pipeline, catalog `main`, schema `orders_dev`/`orders`
- **`sync_lakebase_<target>`** - the Gold -> LakeBase upsert job, scheduled daily at 06:00 UTC
  (paused in `dev`, unpaused in `prod`)
- **`setup_resources_<target>`** - the one-time DDL job, no schedule (run on demand)
- **`generate_synthetic_orders_<target>`** - the fake-order generator, scheduled every 5 minutes,
  **paused in every target** by default

None of these will run successfully yet - two things need to happen first, in order:

**a) Create the `lakebase` secret scope** (needed by both `setup_resources_job` and
`sync_lakebase_job` to connect to Postgres). Two ways to do this - pick one:

*Manually, once, from your own machine:*

```bash
databricks secrets create-scope lakebase
databricks secrets put-secret lakebase jdbc_host
databricks secrets put-secret lakebase jdbc_port
databricks secrets put-secret lakebase jdbc_database
databricks secrets put-secret lakebase jdbc_username
databricks secrets put-secret lakebase jdbc_password
```

Each `put-secret` opens your editor to type the value (or set the equivalent uppercased env var
on the cluster instead - see `get_secret()` in `sync_lakebase.py` for the fallback order).

*Or automatically, via CI* - both `deploy_dev` and `deploy_prod` in `.github/workflows/ci-cd.yml`
run a "Provision lakebase secret scope" step after deploying, which creates the scope (if missing)
and sets all five keys from that environment's GitHub Actions secrets:

```bash
gh secret set LAKEBASE_JDBC_HOST --env dev --repo ravi98sankar/Databricks-pipelines
gh secret set LAKEBASE_JDBC_PORT --env dev --repo ravi98sankar/Databricks-pipelines
gh secret set LAKEBASE_JDBC_DATABASE --env dev --repo ravi98sankar/Databricks-pipelines
gh secret set LAKEBASE_JDBC_USERNAME --env dev --repo ravi98sankar/Databricks-pipelines
gh secret set LAKEBASE_JDBC_PASSWORD --env dev --repo ravi98sankar/Databricks-pipelines
```

(repeat with `--env prod` for prod's own LakeBase instance). Once these are set, every deploy
keeps the secret scope in sync automatically - rotate the LakeBase password by updating the
GitHub secret, and the next deploy pushes the new value, no manual CLI step needed.

**b) Run the setup job once** to create the Unity Catalog landing schema/volume and the LakeBase
target table (both idempotent - safe to re-run):

```bash
databricks bundle run setup_resources_job -t dev
```

Now the rest can run, in whatever order you like:

```bash
# Unpause the generator (or leave it paused and trigger it manually a few times)
databricks jobs unpause --job-id <generate_synthetic_orders_dev job id>
# or, without unpausing anything:
databricks bundle run generate_synthetic_orders_job -t dev

databricks bundle run orders_etl_pipeline -t dev
databricks bundle run sync_lakebase_job -t dev

# Deploy to prod (do this deliberately - the GitHub Actions workflow gates this
# behind a required approval on the `prod` environment)
databricks bundle deploy -t prod
```

## 4. Run / debug from VS Code

1. Install the [Databricks extension for VS Code](https://marketplace.visualstudio.com/items?itemName=databricks.databricks).
2. `Cmd/Ctrl+Shift+P` -> **Databricks: Configure workspace** and point it at the same host you
   authenticated in Section 2, then select (or create) the bundle in this repo.
3. Use the extension's target picker (bottom status bar) to switch between `dev`/`prod` before
   deploying or running from the sidebar - it shells out to the same `databricks bundle` commands
   shown above.
4. To step through `orders_etl.py` or `sync_lakebase.py` against a *live* cluster instead of a
   local Spark session, install `databricks-connect` in its own venv (matching your cluster's DBR
   version) and use the extension's "Run on cluster" / Databricks Connect debug configuration.
   For everyday unit tests, stick with the plain `pyspark` venv from Section 1 - it's faster and
   doesn't need a running cluster.

## 5. Lint and test locally

```bash
flake8 src tests          # line-length/style is configured in setup.cfg (100 cols)
pytest tests              # runs against a local, in-process Spark session
```

`tests/test_transformations.py` covers `LakebaseConnection` / `get_secret` from the sync job.
`tests/test_generate_synthetic_orders.py` covers the fake-order generator's pure logic
(`generate_order` / `generate_batch` / `write_batch`), including that a seed makes a whole batch
reproducible. The `dlt.table`-decorated pipeline functions and the live JDBC/psycopg2/DDL calls
in `sync_lakebase.run()` and `setup_resources.run()` are integration-level and are exercised by
running the deployed pipeline/jobs in a workspace (Section 3), not by this unit suite.

Two tests in `test_transformations.py` (`_standardize_text_columns` / `_deduplicate_orders`) are
currently `@pytest.mark.skip`ped: the `pyspark` build that comes down alongside `databricks-dlt`
has Databricks-internal patches that reject `SparkSession.builder.getOrCreate()` outside
Databricks Connect, so they can't create a local session in this environment. Doesn't affect real
deploys - the actual pipeline always runs against an already-active cluster session.

## 6. CI/CD (`.github/workflows/ci-cd.yml`)

| Job | Trigger | What it does |
|---|---|---|
| `lint_and_test` | PRs, pushes to `develop`/`main`, manual | `flake8 src tests`, `pytest tests` |
| `validate` | PRs, pushes to `develop`/`main`, manual | Installs the Databricks CLI (`databricks/setup-cli`), runs `databricks bundle validate` |
| `deploy_dev` | pushes to `develop`, manual with `target=dev` | `databricks bundle deploy -t dev` against the `dev` environment - no approval required |
| `deploy_prod` | pushes to `main`, manual with `target=prod` | `databricks bundle deploy -t prod` against the `prod` environment - **queued, then paused** until a required reviewer approves it in the Actions run |

A PR only runs `lint_and_test` and `validate` - nothing deploys until it's merged (deploying on
every PR update as well used to double-deploy the same code once on open/update and again on
merge, so that trigger was dropped).

**Manual runs**: Actions tab -> **CI/CD** -> **Run workflow** -> pick the branch and a `target` (`dev`/`prod`).
`lint_and_test`/`validate` always run; whichever `deploy_*` job matches your chosen `target` runs
too (the other one is skipped). `deploy_prod` still respects the `prod` environment's required
reviewer regardless of how it was triggered.

### Branches

- `main` and `develop` both require a pull request with **at least 1 approval** before merging
  (GitHub branch protection). Push directly to either and GitHub will reject it.
- `prod` deploys only from `main`, and only after that branch's protection rules are satisfied
  (the environment's `deployment_branch_policy` is restricted to protected branches).

### Environments and secrets

The workflow reads `DATABRICKS_HOST` / `DATABRICKS_TOKEN` from **GitHub Actions environment
secrets**, not repo-level secrets, so `dev` and `prod` can safely point at different workspaces
or use different service-principal tokens. Set them once per environment:

```bash
gh secret set DATABRICKS_HOST --env dev --repo ravi98sankar/Databricks-pipelines
gh secret set DATABRICKS_TOKEN --env dev --repo ravi98sankar/Databricks-pipelines
gh secret set DATABRICKS_HOST --env prod --repo ravi98sankar/Databricks-pipelines
gh secret set DATABRICKS_TOKEN --env prod --repo ravi98sankar/Databricks-pipelines
```

(or **Settings -> Environments -> [dev/prod] -> Add secret** in the browser). The `prod`
environment also has a required reviewer configured (**Settings -> Environments -> prod ->
Required reviewers**) - without at least one reviewer added there, `deploy_prod` would run
automatically on every push to `main` instead of pausing for a manual approval click.

Both `deploy_dev` and `deploy_prod` also provision the `lakebase` secret scope from five more
environment secrets per target - `LAKEBASE_JDBC_HOST`, `LAKEBASE_JDBC_PORT`,
`LAKEBASE_JDBC_DATABASE`, `LAKEBASE_JDBC_USERNAME`, `LAKEBASE_JDBC_PASSWORD` (see Section 3a).
These are unrelated to `DATABRICKS_HOST`/`DATABRICKS_TOKEN` - those authenticate the CLI to the
workspace; these are the application-level Postgres credentials `sync_lakebase.py` and
`setup_resources.py` read via `dbutils.secrets.get()` at runtime.

## Repo layout

```
.
├── databricks.yml               # Bundle: variables, all 4 resources, dev/prod targets
├── requirements.txt              # pyspark, databricks-dlt, psycopg2-binary, Faker, pytest, flake8
├── setup.cfg                     # flake8 config (max-line-length=100) + pytest pythonpath
├── .github/workflows/ci-cd.yml   # lint_and_test -> validate -> deploy_dev / deploy_prod
├── sql/
│   ├── setup_unity_catalog.sql   # Landing schema + volume DDL (idempotent)
│   └── setup_lakebase.sql        # LakeBase target table DDL (idempotent)
├── src/
│   ├── pipelines/orders_etl.py             # Bronze/Silver/Gold DLT pipeline
│   └── jobs/
│       ├── sync_lakebase.py                # Gold -> LakeBase upsert job
│       ├── setup_resources.py              # Applies sql/*.sql - run once, on demand
│       └── generate_synthetic_orders.py    # Fake-order generator - scheduled, paused by default
└── tests/
    ├── test_transformations.py             # LakebaseConnection / get_secret unit tests
    └── test_generate_synthetic_orders.py   # Generator unit tests
```

## Known gaps / things to adjust before real deployment

- All four resources run on **serverless compute** (`serverless: true` on the pipeline, no
  cluster spec on any job - just an `environment_key`/`environments` block per job for its pip
  dependencies). This is cloud-agnostic by design, but only works if serverless is enabled for
  your workspace; if not, you'll need to add a `job_clusters`/`new_cluster` spec back to each job
  and drop `serverless: true` from the pipeline.
- The `lakebase` secret scope isn't created by the bundle - it must exist in each target
  workspace (Section 3a) before `setup_resources_job` or `sync_lakebase_job` will run successfully.
- `RAW_ORDERS_PATH` in `orders_etl.py` (`/Volumes/main/orders_raw/landing/orders`) is **not**
  parameterized per target - `dev` and `prod` pipelines read from the same landing volume. This
  is also why `generate_synthetic_orders_job` deploys paused in every target and should never be
  manually unpaused in `prod`: there's no separate dev-only landing zone for it to write fake
  data into.
- `sync_lakebase.py`'s Gold table reference **is** parameterized (fixed - it used to be a
  hardcoded `main.orders.gold_daily_customer_metrics`, which meant `sync_lakebase_dev` was
  silently reading `prod`'s schema instead of `orders_dev`). `databricks.yml` now passes
  `--catalog`/`--schema` as job parameters from the bundle's `catalog`/`schema` variables, and
  `sync_lakebase.py` builds the fully-qualified table name from those at runtime.
- This repo is **public** on GitHub (required for branch protection and environment approval
  gates to work on the free plan). There's no live workspace credentials committed, but keep
  that in mind before adding anything sensitive.
- `.gitlab-ci.yml` is still present in the repo but is inert here - GitHub doesn't read it. It's
  kept around only in case this project is ever mirrored into a GitLab instance too.
