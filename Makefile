.PHONY: setup lint format typecheck audit test pipeline check pre-commit-install

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
	$(VENV)/pip-audit -r requirements.txt

test:  ## Run the test suite with the 95% coverage floor
	$(VENV)/python -m pytest

pipeline:  ## Regenerate data, charts, dashboard, and report
	$(VENV)/python src/run_pipeline.py

check: lint format typecheck audit pipeline test  ## Run every gate CI enforces

pre-commit-install:  ## Install the local pre-commit hooks
	$(VENV)/pre-commit install
