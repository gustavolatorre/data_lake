# Changelog

All notable changes are listed here. Follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

The internal `ROADMAP.md` (gitignored) holds the deeper item-by-item story —
this file is the public, summarized view.

## [Unreleased]

### Added
- **CI hardening (Wave E)**: `WORKDIR` refactor in `Dockerfile.spark` to satisfy Trivy DS-0013; `ARCHITECTURE.md`, `CONTRIBUTING.md`, `CHANGELOG.md`, `SECURITY.md`.
- **Quarantine sink** (P3.8): rows with `id IS NULL` go to `nessie.silver.breweries_quarantine` (append-only, partitioned by `quarantine_date`) instead of aborting the run. Added to `iceberg_maintenance` weekly job.
- **OpenLineage** (P3.6): Airflow provider + Spark listener registered by default. Empty `OPENLINEAGE_URL` = log-only mode (no external dependency). Set `OPENLINEAGE_URL=http://marquez:5000` to transmit.
- **Declarative quality runner** (P3.7): YAML rule files under `quality/checks/` consumed by `src/utils/quality_runner.py`. Bronze gets `row_count`, `missing_count(id)`, `unique_count(id)`, plus warn-level missing-percent checks on `name` / `brewery_type`. `fail` severity aborts the run; `warn` severity only logs.

### Changed
- `Dockerfile.spark` now uses three explicit `WORKDIR` blocks instead of `RUN ... cd ...` for the from-source Python 3.12 build.
- **Nessie branching** (P3.1) — Bronze/Silver no longer writes directly to `main`. Each DAG run creates an isolated `etl_bronze_silver_<date>` branch via the new `src/utils/nessie_branch.py` REST helper, the Spark jobs bind to that branch through a `--nessie-ref` CLI flag, and `main` advances only after `merge_branch` succeeds. On upstream failure the `cleanup_branch` task drops the orphan so `main` stays clean. `create_spark_session(nessie_ref=...)` accepts the ref as a keyword argument.
- **Bronze partitioning** (P2.3) moved from `partitionedBy(ingestion_date)` (string) to **hidden** `partitionedBy(days(ingestion_ts))` (timestamp transform). New column `ingestion_ts` derived from `execution_date` keeps idempotency under `overwritePartitions()`. Silver dual-filters on both `ingestion_date` (back-compat) and `ingestion_ts` (gives Iceberg the predicate needed for partition pruning). Existing Bronze tables stay valid — but to migrate the partition spec retroactively, run `ALTER TABLE nessie.bronze.breweries DROP PARTITION FIELD ingestion_date; ALTER TABLE nessie.bronze.breweries ADD PARTITION FIELD days(ingestion_ts);` followed by a `CALL nessie.system.rewrite_data_files(...)`.

## [3.1.0] — 2026-05-28

### Added (P0/P1/P2/P3)
- **P0** — Rotated credentials, removed `admin:admin` defaults, unbound Nessie/Spark from host, pinned Docker images, made Bronze idempotent via `overwritePartitions`, added `.gitattributes` to force LF.
- **P1** — Pushed test coverage from ~53% to ≥85%, added `iceberg_maintenance` weekly DAG, added `_assert_source_not_shrinking` guard in Silver, migrated `dbt run + dbt test` to `dbt build`.
- **P2** — Made `check_null_counts` single-pass, cached the Silver MERGE source, dropped the deprecated `BashOperator` and `secret_key` paths, replaced the Spark connection plugin with `AIRFLOW_CONN_SPARK_DOCKER`, added `make init-secrets`, joined `brewery_type_mapping` seed into `dim_brewery_types`.
- **P3** — Added `.pre-commit-config.yaml`, Dependabot config, and `Security` CI job (`pip-audit` + Trivy fs + Trivy config).

### Fixed
- JWT secret missing from `airflow.env` caused zombie tasks (`pid=None`).
- Em-dash (`—`) in seed broke `dbt build` on Dremio due to COLLATE mismatch — replaced with ASCII hyphen.
- 4 brewery types (`taproom`, `beergarden`, `cidery`, `location`) were missing from the seed, breaking the `relationships` test against real API data.

### Removed
- `plugins/create_spark_connection.py` (Airflow 3 forbids ORM access during plugin load).

### Reverted
- **P1.8** — bcrypt-hashed `simple_auth_passwords.json`. SimpleAuthManager does string-equality on the password, not bcrypt verification. Plaintext restored; full fix tracked as P3.10.
- **P1.15** — Iceberg `format-version=3`. Dremio 25.0.0 cannot read it. Reverted to v2 in both Bronze and Silver.

## [2.0.0] — Before 2026-05-27

Pre-audit baseline. See `git log b2ab079..` for the day-by-day history.
