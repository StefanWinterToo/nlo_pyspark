import pytest
from pyspark.sql import SparkSession
from src.extract import extract_bets
#from src.extract import extract_bets

def test_extract_bets(spark_session):
    # Create a SparkSession for testing
    spark = spark_session.builder.getOrCreate()

    # Perform the extraction
    df = extract_bets(spark)

    # Perform assertions on the extracted DataFrame
    assert "account_id" in df.columns
    assert "legs" in df.columns


# Pytest fixture for creating a SparkSession
@pytest.fixture(scope="session")
def spark_session(request):
    spark = SparkSession.builder.appName("pytest-pyspark").getOrCreate()

    # Set up cleanup after all tests are completed
    def teardown():
        spark.stop()

    request.addfinalizer(teardown)
    return spark

# Can also run "python -m pytest tests" in venv