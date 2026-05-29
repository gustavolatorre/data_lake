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
| Bronze | `nessie.bronze.breweries` (Iceberg v2) | Spark | Explicit `BREWERY_SCHEMA` | `writer.overwritePartitions()` keyed on `ingestion_date` |
| Silver | `nessie.silver.breweries` (Iceberg v2) | Spark `MERGE INTO` | Trimmed analytical schema (10 cols) | Idempotent MERGE; soft-delete via `WHEN NOT MATCHED BY SOURCE` + 20% shrink guard |
| Gold | `gold_dev.{dim_*, fact_*, mart_*}` (Dremio) | dbt-dremio | `schema.yml` per model | dbt incremental (`fact_breweries`) + tables/views; `dbt build` enforces test gates |

### Bronze guarantees
- Append per execution date (partition = `ingestion_date`).
- Cached before count-driven validations to avoid N rescans.
- Schema enforcement on read: `spark.read.schema(BREWERY_SCHEMA).json(...)`.

### Silver guarantees
- `_assert_source_not_shrinking` aborts the MERGE if today's source has dropped
  more than `MAX_SOURCE_SHRINK_RATIO` (20%) vs previous `is_active=true` count.
  Protects against partial API fetches silently deactivating half the table.
- Soft-delete: rows missing from today's source get `is_active=false` and a
  new `updated_at` — they remain queryable, just flagged.

### Gold guarantees
- `dbt build` (not `dbt run` then `dbt test`) — tests are blocking and run
  topologically per resource. A failing test on `stg_silver_breweries`
  skips `dim_*` / `fact_*` / `mart_*`.
- `dim_brewery_types` LEFT JOINs `brewery_type_mapping` seed for descriptions;
  the `relationships` test flags any Silver type missing from the seed.

## 3. Orchestration

Three reactive (asset-aware) DAGs + one weekly maintenance DAG:

| DAG | Schedule | Outlets | Inlets |
|-----|----------|---------|--------|
| `staging_ingestion` | `@daily` | `s3://staging/breweries` | — |
| `bronze_silver_processing` | asset `s3://staging/breweries` | `iceberg://nessie/silver/breweries` | (asset) |
| `gold_dbt_processing` | asset `iceberg://nessie/silver/breweries` | `iceberg://nessie/gold/breweries` | (asset) |
| `iceberg_maintenance` | `@weekly` | — | — |

Failure callbacks: shared factory in `dags/callbacks.py`
(`build_failure_callback("LAYER NAME")`).

Spark connection is wired via env var (`AIRFLOW_CONN_SPARK_DOCKER` on
`x-airflow-common`), not a plugin — Airflow 3 forbids ORM writes during
plugin `on_load`.

## 4. Catalog / version control

Project Nessie is the Iceberg catalog. The pipeline currently uses a single
`main` branch — Nessie branching for isolated transformations + zero-copy
rollback is on the roadmap (P3.1).

Iceberg `format-version=2` everywhere. v3 was attempted (P1.15) but Dremio
25.0.0 cannot read it; will revisit when Dremio adds support.

## 5. Quality gates

| Gate | Where | What it catches |
|------|-------|-----------------|
| Pre-commit | local | Format, lint, large files, secrets |
| `Lint` CI job | PR | Ruff (`check` + `format --check`), mypy |
| `Test` CI job | PR | 84+ unit tests, coverage gate `fail_under=85` |
| `dbt Validate` CI job | PR | `dbt deps + parse` via `dbt-duckdb` (connectionless) |
| `Security` CI job | PR | `pip-audit`, Trivy fs (HIGH+), Trivy config (MEDIUM+) — `continue-on-error` while burning down baseline |
| `_assert_source_not_shrinking` | Silver Spark job | Partial fetch protection |
| `check_null_counts(..., fail_on_nulls=True)` | Silver Spark job | NULL id detection |
| `dbt build` tests (`unique`, `not_null`, `relationships`, `freshness`) | Gold Airflow task | Cross-layer integrity |

## 5b. Observability — OpenLineage (P3.6)

Two emitters are wired by default; both are inert until a collector URL is
configured, so the bundled images "just work" with no external dependency.

| Emitter | Source | Triggered by |
|---------|--------|--------------|
| Airflow provider | `apache-airflow-providers-openlineage` (image-baked) | every task instance |
| Spark listener | `openlineage-spark_2.13` JAR + `spark.extraListeners` in `create_spark_session` | every Spark action |

**Configuration:**

| Env var | Default | Effect |
|---------|---------|--------|
| `OPENLINEAGE_URL` | empty | Empty = listener loads but only logs; non-empty (e.g. `http://marquez:5000`) flips to HTTP transmit |
| `OPENLINEAGE_NAMESPACE` | `data_lake` | Groups events from this project across pipelines / Spark apps |
| `AIRFLOW__OPENLINEAGE__NAMESPACE` | `data_lake` | Same, but for the Airflow provider scope |

**Why empty-by-default:** keeps the smoke test reproducible (no external
service to fail), and surfaces lineage in logs as a learning aid. Point at
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
| Silver MERGE refuses with `SourceShrinkError` | Today's API fetch was partial. Investigate `staging_ingestion` task logs before clearing |

## 8. Roadmap pointer

The living roadmap is `ROADMAP.md` (gitignored — local working doc).
Architecture-affecting items: P3.1 (Nessie branching), P3.5 (e2e tests),
P3.10 (SimpleAuthManager replacement), P3.6 (OpenLineage), P3.7 (Great
Expectations / Soda).
