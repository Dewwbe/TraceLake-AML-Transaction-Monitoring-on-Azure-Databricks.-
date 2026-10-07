# Assumptions and limitations

## Execution status

* `DeltaStore` (including the SCD2 MERGE and insert-only merges), Auto Loader, `init_tables` and the Databricks Asset Bundle jobs **have been run end to end** on Azure Databricks serverless compute against the bundled synthetic data. Automated tests still use plain Spark with parquet.
* Not run: the Airflow DAG and the GitHub deploy workflow. Airflow operator arguments (`job_name`, `python_named_params`) must be checked against the installed provider version.
* The Databricks run used one small synthetic dataset; no scale or performance testing was done on Databricks.

## Data assumptions

* SAML-D column names, timestamp format and currency/country labels are recalled and unverified; the Bronze reader validates the CSV header and fails fast.
* FX rates are placeholders (`reference/fx_rates_illustrative.csv`). AED amounts are not suitable for real analysis until replaced by a sourced table.
* Customers, accounts and the ground truth for screening are synthetic. SAML-D has no customer data.
* The bundled sanctions list is fictional. The OFAC converter is tested against a fixture written to the assumed layout.

## Method limitations

* All rule thresholds, risk weights and screening bands are illustrative.
* Streaming covers Bronze ingestion only; Silver and rules are incremental micro-batches, not stateful streaming.
* Case workflow (assignment, disposition, closure) is simulated from ground-truth labels and is only for dashboards.
* Local Spark tests ran on 1 CPU and 4 GB; no performance or scale testing was done.
* Name screening is name-only with Latin-script normalisation; recall figures apply to six synthetic perturbation types only.
* `pandas<3` is pinned because PySpark warned about pandas 3 support.
