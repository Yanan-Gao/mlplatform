import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    spark = (SparkSession.builder
                .master("local")
                .appName("pyspark-features-test")
                .config("--files", "weights.zip")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.execution.arrow.maxRecordsPerBatch", 512)
                .getOrCreate())

    yield spark

    spark.stop()
