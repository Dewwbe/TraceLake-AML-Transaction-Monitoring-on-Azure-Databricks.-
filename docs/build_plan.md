# Build plan

Effort is relative (S / M / L) and indicative, not measured. Phase 0 is complete in this base repository.

| Phase | Scope | Deliverables | Exit criteria | Effort |
|---|---|---|---|---|
| 0 Foundations | Repo, local run, tests, CI | This repository | `make local-run` and `make test` pass | Done |
| 1 Azure environment | Resource group, ADLS Gen2, Databricks workspace, Unity Catalog catalog and landing volume, budget alert, auth for CI | Environment notes, bundle host set | `init_tables` creates all Delta tables | M |
| 2 Ingestion | Deploy bundle, run replayer to landing, Auto Loader stream, reference loads | Populated Bronze | Re-running adds no duplicate rows; header mismatch fails fast | M |
| 3 Silver and DQ | Run on SAML-D after verifying the profile; extend currency aliases; tune DQ thresholds | DQ report, quarantine review | Counts reconcile (raw = valid + quarantined + duplicates) | M |
| 4 Gold and SCD2 | Delta MERGE SCD2 for customer and country; dim_date, facts | Queryable history | One current row per customer; no gaps between versions | M |
| 5 Detection | Load real country-risk table; calibrate rules against labels; screening against OFAC or fictional list | `mart_rule_performance` | Documented precision/recall and chosen thresholds | L |
| 6 Serving | Databricks SQL views (`sql/analytics`), Power BI report on Gold | Dashboard | Rule performance, alert ageing and case mix visible | M |
| 7 Hardening | Deploy Airflow DAG, validate bundle, CI deploy, README with screenshots, cost teardown | Release | One-command deploy and teardown | M |

## Extensions (not built)

AMLworld adapter, stateful streaming rules, DOB/nationality corroboration in screening, graph features, ML scoring with scikit-learn, Purview lineage, data contracts.

## Risks

| Risk | Mitigation |
|---|---|
| SAML-D licence or schema differs from assumption | Verify before Phase 3; profile is configuration only |
| Azure cost overrun | Single-node job clusters, PAUSED continuous job, budget alert |
| Untested Delta paths fail on first run | Phase 2 and 4 treat them as integration work; parquet path is the reference behaviour |
