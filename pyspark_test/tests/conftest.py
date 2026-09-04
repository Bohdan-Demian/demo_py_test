import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():

    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        # Databricks
        session = SparkSession.getActiveSession()

        if session is None:
            session = SparkSession.builder.getOrCreate()

        yield session

    else:
        # Local
        session = (
            SparkSession.builder
            .appName("pyspark-ecommerce-quality-tests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

        yield session
        session.stop()
