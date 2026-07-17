.PHONY: setup lint format typecheck audit test pipeline causal network validate check pre-commit-install

VENV := .venv/bin

setup:  ## Create the venv and install dev dependencies
	python3 -m venv .venv
	$(VENV)/python -m pip install --upgrade pip
	$(VENV)/python -m pip install -r requirements-dev.txt

lint:  ## Lint (ruff)
	$(VENV)/ruff check src tests

format:  ## Check formatting (ruff)
	$(VENV)/ruff format --check src tests

typecheck:  ## Static type check (mypy)
	$(VENV)/mypy

audit:  ## Dependency vulnerability audit (pip-audit)
	$(VENV)/pip-audit -r requirements-dev.txt

test:  ## Run the test suite with the 95% coverage floor
	$(VENV)/python -m pytest

pipeline:  ## Regenerate analytics, publications, catalog, lineage, and object manifest
	$(VENV)/python -m src

causal:  ## Rebuild registered RCT/DiD evidence
	$(VENV)/python -m src.causal_evaluation

network:  ## Rebuild the constrained multi-echelon plan
	$(VENV)/python -m src.network_optimization

validate:  ## Re-run analytical and publication release gates
	$(VENV)/python -m src.sql_quality_gate
	$(VENV)/python -m src.pre_delivery_validation
	$(VENV)/python -m src.ci_quality_gate

check: lint format typecheck audit pipeline test  ## Run every gate CI enforces

pre-commit-install:  ## Install the local pre-commit hooks
	$(VENV)/pre-commit install
