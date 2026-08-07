# Databricks Pipelines

Medallion-architecture orders pipeline built with **Databricks Asset Bundles (DABs)**:

- `src/pipelines/orders_etl.py` - Lakeflow Declarative Pipeline (DLT). Bronze (Auto Loader
  ingestion) -> Silver (validated/deduplicated orders) -> Gold (daily customer spend metrics).
- `src/jobs/sync_lakebase.py` - Scheduled job that reads the Gold table from Unity Catalog and
  upserts it into a LakeBase (serverless Postgres) table over JDBC.
- `databricks.yml` - The bundle definition: `dev` and `prod` targets, the pipeline resource, and
  the job resource (with its own job cluster and schedule).
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
`psycopg2-binary`, `pytest`, and `flake8` — everything needed to lint and test the code without a
live workspace. **Do not** add `databricks-connect` to this same environment; it conflicts with
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

# Deploy the pipeline + job to your dev workspace
databricks bundle deploy -t dev

# Run the pipeline or job once, on demand
databricks bundle run orders_etl_pipeline -t dev
databricks bundle run sync_lakebase_job -t dev

# Deploy to prod (do this deliberately - the GitHub Actions workflow gates this
# behind a required approval on the `prod` environment)
databricks bundle deploy -t prod
```

`bundle deploy` uploads the source files under `src/` and creates/updates:

- a Lakeflow Declarative Pipeline named `orders_etl_<target>` in catalog `main`
  (schema `orders_dev` for `dev`, `orders` for `prod`)
- a job named `sync_lakebase_<target>` on its own single-node job cluster, scheduled daily at
  06:00 UTC (paused in `dev`, unpaused in `prod`)

Before the `sync_lakebase` job can actually connect to LakeBase, create a secret scope named
`lakebase` in the target workspace with keys `jdbc_host`, `jdbc_port`, `jdbc_database`,
`jdbc_username`, `jdbc_password` (or set the equivalent uppercased env vars on the cluster - see
`get_secret()` in `sync_lakebase.py` for the fallback order).

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

`tests/test_transformations.py` covers the pure helpers that don't need a live workspace:
`_standardize_text_columns` / `_deduplicate_orders` from the pipeline, and
`LakebaseConnection` / `get_secret` from the sync job. The `dlt.table`-decorated pipeline
functions and the live JDBC/psycopg2 calls in `sync_lakebase.run()` are integration-level and are
exercised by running the deployed pipeline/job in a workspace (Section 3), not by this unit suite.

## 6. CI/CD (`.github/workflows/ci-cd.yml`)

| Job | Trigger | What it does |
|---|---|---|
| `lint_and_test` | PRs, pushes to `develop`/`main` | `flake8 src tests`, `pytest tests` |
| `validate` | PRs, pushes to `develop`/`main` | Installs the Databricks CLI (`databricks/setup-cli`), runs `databricks bundle validate` |
| `deploy_dev` | PRs, pushes to `develop` | `databricks bundle deploy -t dev` against the `dev` environment - no approval required |
| `deploy_prod` | pushes to `main` | `databricks bundle deploy -t prod` against the `prod` environment - **queued, then paused** until a required reviewer approves it in the Actions run |

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

## Repo layout

```
.
├── databricks.yml               # Bundle: variables, pipeline + job resources, dev/prod targets
├── requirements.txt              # pyspark, databricks-dlt, psycopg2-binary, pytest, flake8
├── setup.cfg                     # flake8 config (max-line-length=100) + pytest pythonpath
├── .github/workflows/ci-cd.yml   # lint_and_test -> validate -> deploy_dev / deploy_prod
├── src/
│   ├── pipelines/orders_etl.py   # Bronze/Silver/Gold DLT pipeline
│   └── jobs/sync_lakebase.py     # Gold -> LakeBase upsert job
└── tests/
    └── test_transformations.py   # Unit tests for the pure helper functions above
```

## Known gaps / things to adjust before real deployment

- `databricks.yml`'s job cluster uses `node_type_id: Standard_DS3_v2` (Azure). Swap it for an
  AWS/GCP instance type if your workspace isn't on Azure.
- The `lakebase` secret scope isn't created by the bundle - it must exist in each target
  workspace before `sync_lakebase_job` will run successfully.
- `RAW_ORDERS_PATH` in `orders_etl.py` (`/Volumes/main/orders_raw/landing/orders`) is a
  placeholder Unity Catalog volume - point it at your real landing zone.
- This repo is **public** on GitHub (required for branch protection and environment approval
  gates to work on the free plan). There's no live workspace credentials committed, but keep
  that in mind before adding anything sensitive.
- `.gitlab-ci.yml` is still present in the repo but is inert here - GitHub doesn't read it. It's
  kept around only in case this project is ever mirrored into a GitLab instance too.
