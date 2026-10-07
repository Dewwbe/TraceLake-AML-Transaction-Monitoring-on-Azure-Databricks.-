.PHONY: setup lint test docs sample-data local-run report bundle-validate clean
PY ?= python

setup:
	$(PY) -m pip install -e ".[dev]"

lint:
	ruff check .

test:
	$(PY) -m pytest -q

docs:
	$(PY) scripts/generate_docs.py

sample-data:
	$(PY) scripts/generate_sample_data.py

local-run:
	$(PY) -m aml.jobs.local_pipeline

report:
	$(PY) -m aml.jobs.report

bundle-validate:
	databricks bundle validate -t dev

clean:
	rm -rf data/landing data/lake data/checkpoints data/ground_truth
