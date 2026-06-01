# Architecture

> Living document. Update whenever the topology, layer contract, or major
> integration changes. The README is the marketing/onboarding view; this is
> the engineering view.

## 1. High-level topology

```
                            ┌────────────────────┐
                            │  OpenBreweryDB API │
                            └─────────┬──────────┘
                                      │ HTTPS, paginated
                                      ▼
        ┌──────────┐   put_object   ┌─────────┐   s3a://staging/   ┌────────────┐
        │  Airflow │───────────────▶│  MinIO  │◀───────────────────│  Spark 4   │
        │   3.2.1  │                │ (bronze │                    │ (master +  │
        │ Scheduler│                │ warehs.)│   s3a://warehouse/ │   worker)  │
        │ + DAG    │◀────────────── │         │ ─────────────────▶ │            │
        │ Processor│   asset event  └─────────┘    Iceberg files   └─────┬──────┘
        │ + WebSrv │                                                      │
        └────┬─────┘                                                      │
             │                                                            │
             │ submit                                                     │ register tables
             ▼                                                            ▼
        ┌─────────┐                                                 ┌─────────┐
        │  Spark  │   read/write    ┌─────────┐    REST API         │  Nessie │
        │ submits │────────────────▶│ Iceberg │◀───────────────────▶│ catalog │
        └─────────┘                 │ tables  │                     │  v2 API │
                                    └─────────┘                     └────┬────┘
                                         ▲                               │
                                         │ dbt build                     │ source
                                         │                               ▼
                                    ┌────┴────┐                    ┌──────────┐
                                    │  dbt-   │                    │  Dremio  │
                                    │ dremio  │◀──── SQL (TCP) ────│ 25.0.0   │
                                    └─────────┘                    └──────────┘
```

11 long-running containers in `docker-compose.yml` + 2 one-shot setup
containers (`minio-setup`, `dremio-setup`).

## 2. Layer contract (Medallion)

| Layer | Path | Engine | Schema discipline | Idempotency |
|-------|------|--------|-------------------|-------------|
| Staging | `s3://staging/breweries/{date}/breweries_page_{N}.json` | Python + MinIO SDK | Raw JSON | Full overwrite per day |
| Bronze | `nessie.bronze.breweries` (Iceberg v2) | Spark | Explicit `BREWERY_SCHEMA` + `ingestion_ts` for partition spec | `writer.overwritePartitions()` keyed on `days(ingestion_ts)` (hidden) |
| Silver | `nessie.silver.breweries` (Iceberg v2) | Spark `MERGE INTO` | Trimmed analytical schema (10 cols) | Idempotent MERGE; soft-delete via `WHEN NOT MATCHED BY SOURCE` + 20% shrink guard |
| Gold | `gold_dev.{dim_*, fact_*, mart_*}` (Dremio) | dbt-dremio | `schema.yml` per model | dbt incremental (`fact_breweries`) + tables/views; `dbt build` enforces test gates |

### Bronze guarantees
- Hidden partitioning by `days(ingestion_ts)` (Iceberg transform). `ingestion_ts`
  is derived from the Airflow `execution_date`, so re-runs land on the same
  partition and `overwritePartitions()` stays idempotent. The lock-step
  `ingestion_date` string is kept as a regular column for query / join
  ergonomics; Silver filters by **both** to ensure Iceberg prunes correctly.
- Cached before count-driven validations to avoid N rescans.
- Schema enforcement on read: `spark.read.schema(BREWERY_SCHEMA).json(...)`.

### Silver guarantees
- `_assert_source_not_shrinking` aborts the MERGE if today's source has dropped
  more than `MAX_SOURCE_SHRINK_RATIO` (20%) vs previous `is_active=true` count.
  Protects against partial API fetches silently deactivating half the table.
- Soft-delete: rows missing from today's source get `is_active=false` and a
  new `updated_at` — they remain queryable, just flagged.
- **Quarantine sink** (`nessie.silver.breweries_quarantine`, append-only,
  partitioned by `quarantine_date`): rows that fail row-level rules (currently
  `id IS NULL`) are diverted here instead of aborting the run. The MERGE then
  proceeds on the cleaned subset. Each quarantined row carries a stable
  `quarantine_reason` code (e.g. `NULL_ID`) for downstream alerting.

### Gold guarantees
- `dbt build` (not `dbt run` then `dbt test`) — tests are blocking and run
  topologically per resource. A failing test on `stg_silver_breweries`
  skips `dim_*` / `fact_*` / `mart_*`.
- `dim_brewery_types` LEFT JOINs `brewery_type_mapping` seed for descriptions;
  the `relationships` test flags any Silver type missing from the seed.

## 3. Orchestration

Two Medallion pipelines — breweries (full Bronze→Gold) and brasileirão
(Bronze→Silver, see §8) — plus a shared weekly maintenance DAG. All
inter-layer scheduling is asset-aware (reactive):

| DAG | Schedule | Outlets | Inlets |
|-----|----------|---------|--------|
| `staging_breweries_ingestion` | `@daily` | `s3://staging/breweries` | — |
| `bronze_silver_breweries_processing` | asset `s3://staging/breweries` | `iceberg://nessie/silver/breweries` (emitted by `merge_branch`) | (asset) |
| `gold_dbt_breweries_processing` | asset `iceberg://nessie/silver/breweries` | `iceberg://nessie/gold/breweries` | (asset) |
| `staging_brasileirao_ingestion` | `@daily` | `s3://staging/brasileirao` | — |
| `bronze_silver_brasileirao_processing` | asset `s3://staging/brasileirao` | `iceberg://nessie/silver/brasileirao` (emitted by `merge_branch`) | (asset) |
| `iceberg_maintenance` | `@weekly` | — | — |

Failure callbacks: shared factory in `dags/callbacks.py`
(`build_failure_callback("LAYER NAME")`).

Spark connection is wired via env var (`AIRFLOW_CONN_SPARK_DOCKER` on
`x-airflow-common`), not a plugin — Airflow 3 forbids ORM writes during
plugin `on_load`.

## 4. Catalog / version control

Project Nessie is the- **Isolated Git-like branching for Bronze and Silver**. Every Bronze/Silver
  DAG run carves out its own Nessie branch** before any Spark write
and merges it back into `main` only after the Silver MERGE succeeds.

```
main ───────────────────────────────────────●─────▶ (Gold reads here)
                                            ▲
                                            │ merge_branch (success path)
            create_branch                   │
main ──●────────── etl_bronze_silver_<date> ─┘
       └──────▶  staging_to_bronze ▶ bronze_to_silver ▶ merge_branch
                                          │
                                          ▼ on any failure
                                    cleanup_branch
                                    (drops the branch — main untouched)
```

`src/utils/nessie_branch.py` hits the Nessie v2 REST API directly (no
`nessie-spark-extensions` JAR needed). Branch names are deterministic
(`etl_bronze_silver_YYYY_MM_DD`) so re-runs are idempotent: the second
attempt re-uses the existing branch instead of failing.

`create_spark_session(app_name, nessie_ref=...)` binds the catalog to a
specific ref; both `ingest_breweries.py` and `transform_breweries.py`
accept a `--nessie-ref` CLI flag that the DAG fills in from a Jinja
template. Gold (dbt-dremio) continues to read `main`.

Iceberg `format-version=2` everywhere. v3 was attempted (P1.15) but Dremio
25.0.0 cannot read it; will revisit when Dremio adds support.

## 5. Quality gates

| Gate | Where | What it catches |
|------|-------|-----------------|
| Pre-commit | local | Format, lint, large files, secrets |
| `Lint` CI job | PR | Ruff (`check` + `format --check`), mypy |
| `Test` CI job | PR | 200+ unit tests, coverage gate `fail_under=85` |
| `dbt Validate` CI job | PR | `dbt deps + parse` via `dbt-duckdb` (connectionless) |
| `Security` CI job | PR | `pip-audit`, Trivy fs (HIGH+), Trivy config (MEDIUM+) — `continue-on-error` while burning down baseline |
| `_assert_source_not_shrinking` | Silver Spark job | Partial fetch protection |
| Quarantine split (`id IS NULL`) | Silver Spark job | Diverts bad rows to `breweries_quarantine` instead of aborting |
| Declarative YAML rules via `run_quality_checks` (P3.7) | Bronze Spark job | Row count, missing-count, unique-count, missing-percent — `fail` rules abort, `warn` rules only log |
| `dbt build` tests (`unique`, `not_null`, `relationships`, `freshness`) | Gold Airflow task | Cross-layer integrity |

## 5b. Observability — OpenLineage (P3.6)

Two emitters, with **different empty-URL behaviour**. Both keep the bundled
images working with no external dependency:

| Emitter | Source | Triggered by | When `OPENLINEAGE_URL` is empty (default) |
|---------|--------|--------------|-------------------------------------------|
| Airflow provider | `apache-airflow-providers-openlineage` (image-baked) | every task instance | Provider stays loaded, log-only — nothing transmitted |
| Spark listener | `openlineage-spark_2.13` JAR + `spark.extraListeners` in `create_spark_session` | every Spark action | **Not registered at all** — `_apply_openlineage_config` returns the builder untouched (silent no-op) |

**Configuration:**

| Env var | Default | Effect |
|---------|---------|--------|
| `OPENLINEAGE_URL` | empty | Empty = Airflow provider logs only / Spark listener not registered. Non-empty (e.g. `http://marquez:5000`) registers the Spark listener and flips both to HTTP transmit |
| `OPENLINEAGE_NAMESPACE` | `data_lake` | Groups events from this project across pipelines / Spark apps |
| `AIRFLOW__OPENLINEAGE__NAMESPACE` | `data_lake` | Same, but for the Airflow provider scope |

**Why the Spark listener is not registered when empty:** the Spark driver runs
`spark-submit` in client mode from the Airflow container, where the listener
JAR is *not* on the classpath. Registering `spark.extraListeners`
unconditionally would crash the driver with `ClassNotFoundException`. So
`OPENLINEAGE_URL` doubles as the opt-in switch — when you set it, you also take
responsibility for making the JAR reachable on the driver (e.g. via `--jars`).
This keeps the smoke test reproducible (no external service to fail). Point at
Marquez / Datadog / any OpenLineage-compatible collector when you're ready.

## 6. Credentials & secrets

Local development only. Three categories:

| Where | What |
|-------|------|
| `.env` (gitignored) | MinIO, Postgres, Dremio admin creds |
| `airflow.env` (gitignored) | Fernet key, API secret key (`[api] secret_key`), JWT secret, DB SQLAlchemy URL |
| `simple_auth_passwords.json` (container-internal, never mounted) | Plaintext Airflow login password — SimpleAuthManager does NOT support hashing. Tracked as P3.10 (auth manager replacement) |

Bootstrap helper: `make init-secrets` copies `airflow.env.example` to
`airflow.env` with freshly generated Fernet + Webserver + JWT keys.

## 7. Where to look when something breaks

| Symptom | First place to check |
|---------|----------------------|
| Login 401 on `localhost:8080` | `simple_auth_passwords.json` inside the webserver container; `AIRFLOW_USER` / `AIRFLOW_PASSWORD` in `.env` |
| Task "zombie" (state queued + executor failed) | JWT secret mismatch between containers — `AIRFLOW__API_AUTH__JWT_SECRET` in `airflow.env` |
| `dbt build` runtime error on seed load with VARCHAR/COLLATE | Non-ASCII char in `brewery_type_mapping.csv` (lesson from PR #18) |
| `Failed to load plugin spark_connection_plugin` | You're on a pre-PR#17 image; rebuild — plugin was removed in favor of `AIRFLOW_CONN_*` |
| `dremio-setup` crash-loop with `syntax error: unexpected word` | CRLF line endings in `setup_sources.sh` (Windows checkout without `.gitattributes` LF coercion). Fix: re-clone or `git rm --cached -r . && git reset --hard` |
| Silver MERGE refuses with `SourceShrinkError` | Today's API fetch was partial. Investigate `staging_breweries_ingestion` task logs before clearing |
| Rows missing from Silver but present in Bronze | Check `nessie.silver.breweries_quarantine WHERE quarantine_date = '<date>'` — they probably failed `id IS NULL` |

## 8. Second domain — Brasileirão Série A

A second, independent Medallion pipeline runs on the same stack, ingesting
Brazilian football (Brasileirão Série A) match data. It reuses every shared
util (`create_spark_session`, `nessie_branch`, `quality_runner`,
`minio_client`, `logging_config`) but **stops at Silver** — there is no Gold /
dbt model for it yet (breweries remains the only Gold domain).

### Layer contract

| Layer | Path | Engine | Idempotency |
|-------|------|--------|-------------|
| Staging | `s3://staging/brasileirao/{match_date}/matches.json` | Python + MinIO SDK | Overwrite per match-date |
| Bronze | `nessie.bronze.brasileirao` (Iceberg v2) | Spark | `overwritePartitions()` on `days(ingestion_ts)`; multi-date scan filtered by a Bronze high-watermark (`MAX(ingestion_date)`, `>=` so the HW date is re-processed) |
| Silver | `nessie.silver.brasileirao` (Iceberg v2) | Spark `MERGE INTO` | Upsert keyed on `ge_match_id`; partitioned by `months(match_date)` |

### Key differences vs the breweries pipeline

- **Source**: the GE (Globo Esporte) internal JSON API. `fetch_brasileirao`
  scans all 38 rounds, keeps only finished matches, and partitions staging
  files by the **match date** (not by Airflow's `execution_date`). First run
  backfills every historical date; later runs upload only `execution_date`.
- **Bronze is path-driven, not execution-date-driven.** A single staging run
  can drop many dates at once, so Bronze scans
  `s3a://staging/brasileirao/*/matches.json`, derives `ingestion_date` from
  the file path, and filters with a high-watermark. `ge_match_id` is a
  UUID-style **string** (not an int).
- **Silver is a plain UPSERT** — no `WHEN NOT MATCHED BY SOURCE` (a played
  match never disappears) and no shrink guard (staging is incremental, not a
  full snapshot). The Bronze→Silver delta is driven by a Silver-side
  high-watermark on `MAX(updated_at)`.
- **Enrichment**: derives `stadium_state` (UF) + `stadium_state_origin` via
  broadcast-joined lookup dicts in `src/silver/stadium_enrichment.py`, with a
  cascade `stadium → home_team → __UNKNOWN__`. Also derives `total_goals` and
  `match_outcome` (HOME_WIN / AWAY_WIN / DRAW).
- **Quarantine**: rows with NULL `ge_match_id` go to
  `nessie.silver.brasileirao_quarantine` (append-only, partitioned by
  `quarantine_date`).
- **DQ contracts**: `quality/checks/bronze_brasileirao.yml` is intentionally
  minimal (`row_count >= 1`); `silver_brasileirao.yml` enforces 7 fail-level
  invariants (no-null + unique `ge_match_id`, non-null `match_date`,
  `stadium_state`, `is_active`, …) plus 3 warn-level `missing_percent` signals.

### Orchestration

Same Nessie branch-isolation pattern as breweries:
`create_branch` → `staging_to_bronze` → `bronze_to_silver` → `merge_branch`,
with `cleanup_branch` (`trigger_rule=one_failed`, wired off `staging_to_bronze`
and `bronze_to_silver`) dropping the orphan branch on failure so `main` stays
clean. Branch name: `etl_bronze_silver_brasileirao_<date>`.

## 9. Roadmap pointer

The living roadmap is `ROADMAP.md` (gitignored — local working doc).
Architecture-affecting items: P3.1 (Nessie branching), P3.5 (e2e tests),
P3.10 (SimpleAuthManager replacement), P3.6 (OpenLineage), P3.7 (Great
Expectations / Soda).
