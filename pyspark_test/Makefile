PYTHON ?= $(shell test -x .venv/bin/python && echo .venv/bin/python || echo python3)
PYTHONPATH := src
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
PATH := $(JAVA_HOME)/bin:$(PATH)
LINT_PATHS := src/validation tests notebooks setup.py

export PYTHONPATH
export JAVA_HOME
export PATH

.PHONY: test databricks-test lint format check

test:
	$(PYTHON) -m pytest

databricks-test:
	$(PYTHON) -m pytest -m "not local_only" -p no:cacheprovider -q

lint:
	$(PYTHON) -m ruff check $(LINT_PATHS)
	$(PYTHON) -m ruff format --check $(LINT_PATHS)

format:
	$(PYTHON) -m ruff check --fix $(LINT_PATHS)
	$(PYTHON) -m ruff format $(LINT_PATHS)

check: lint test
