PYTHON ?= $(shell test -x .venv/bin/python && echo .venv/bin/python || echo python3)
PYTHONPATH := src
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
PATH := $(JAVA_HOME)/bin:$(PATH)

export PYTHONPATH
export JAVA_HOME
export PATH

.PHONY: test databricks-test

test:
	$(PYTHON) -m pytest

databricks-test:
	$(PYTHON) -m pytest -m "not local_only" -p no:cacheprovider -q
