import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    """Fixture to create a local PySpark session for tests."""
    session = (
        SparkSession.builder.appName("PySpark-CI-Test")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_clean_data(spark):
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])

    data = [
        ("Alice", 100.0),   # Valid record
        ("Bob", -50.0),     # Invalid: amount <= 0
        ("Charlie", 0.0),    # Invalid: amount <= 0
        (None, 200.0),      # Invalid: NULL name
        (None, -10.0),      # Invalid: both NULL name and amount <= 0
    ]

    input_df = spark.createDataFrame(data, schema)
    result_df = clean_data(input_df)

    rows = result_df.collect()

    # 1. Verify valid records are kept and invalid ones filtered
    assert len(rows) == 1

    row = rows[0]
    # 2. Verify null names removed & valid names kept
    assert row["name"] == "Alice"
    
    # 3. Verify amount > 0 kept
    assert row["amount"] == 100.0
    
    # 4. Verify amount_with_tax calculation (100 * 1.20 = 120.0)
    assert row["amount_with_tax"] == pytest.approx(120.0, rel=1e-5)