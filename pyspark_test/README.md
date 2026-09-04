# PySpark E-commerce Quality Pipeline

Навчальний Big Data QA проєкт для практики PySpark і тестів навколо ETL pipeline.

## Що є всередині

- `data/raw/` - маленькі sample CSV datasets.
- `src/ecommerce_quality/` - PySpark код для ingestion, cleaning, transformation і quality checks.
- `tests/` - приклади unit та integration tests через `pytest`.
- `pyproject.toml` - залежності та налаштування pytest.

## Бізнес-сценарій

Pipeline читає e-commerce події, користувачів і товари, чистить raw data, відокремлює погані записи, будує:

- `fact_orders`
- `daily_sales`
- `customer_summary`
- `bad_records`

## Швидкий старт

PySpark потребує Java Runtime. На macOS найпростіший варіант:

```bash
brew install openjdk@17
```

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Якщо у твоєму середовищі старий `pip` не підтримує editable install, можна використати простіший шлях:

```bash
pip install pyspark chispa pytest
PYTHONPATH=src pytest
```

Запуск pipeline локально:

```bash
make pipeline
```

Або напряму:

```bash
source .env.example
python3 -m ecommerce_quality.pipeline \
  --events data/raw/events.csv \
  --users data/raw/users.csv \
  --products data/raw/products.csv \
  --output data/processed
```

## Що тестувати як Big Data QA

- schema validation
- null handling
- duplicate handling
- invalid records
- joins між fact і dimension tables
- aggregation correctness
- end-to-end pipeline output

## Databricks demo run

Для демо запуску тестів у Databricks Repo відкрий і запусти notebook:

```text
run_test_notebook.ipynb
```

Notebook:

- встановлює `pytest` і `chispa`
- додає `src/` у `sys.path`
- використовує активну Databricks SparkSession
- запускає тести командою `pytest`
- пропускає тести з marker `local_only`
- зберігає latest run summary у Delta table `pyspark_demo_test_run_results`
- зберігає test case details у Delta table `pyspark_demo_test_case_results`

Тест [tests/test_pipeline_integration.py](tests/test_pipeline_integration.py) позначений як `local_only`, бо він використовує локальні filesystem шляхи і `tmp_path`. Для Databricks demo краще запускати unit-style Spark tests з in-memory DataFrame.

Після запуску тестів відкрий notebook-dashboard:

```text
latest_test_run_dashboard.ipynb
```

Він показує latest run summary, список test cases для останнього запуску і коротку історію запусків.
