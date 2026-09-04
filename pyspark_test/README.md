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
