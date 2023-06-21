import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.transform import add_outcomes_to_bets_bronze, trans_transactions

def test_add_outcomes_to_bets_bronze(spark_session):
    # Create a SparkSession for testing
    spark = spark_session.builder.getOrCreate()

    # Create a sample DataFrame for testing
    data = [
        ([1, 10], [2, 20])
    ]
    columns = ["legs", "markets"]
    df = spark.createDataFrame(data, columns)

    # Perform the transformation
    transformed_df = add_outcomes_to_bets_bronze(df)

    # Perform assertions on the transformed DataFrame
    assert "outcomes" in transformed_df.columns

    # Additional assertions or checks can be added based on specific requirements
    # ...


def test_trans_transactions(spark_session):
    # Create a SparkSession for testing
    spark = spark_session.builder.getOrCreate()

    # Create a sample DataFrame for testing
    data = [
        (1, "A", 10),
        (1, "B", 20),
        (2, "C", 30),
        (2, "D", 40),
        (3, "E", 50),
        (3, "F", 60)
    ]
    columns = ["sportsbook_id", "column1", "column2"]
    df = spark.createDataFrame(data, columns)

    # Perform the transformation
    transformed_df = trans_transactions(df)

    # Perform assertions on the transformed DataFrame
    assert "transactions" in transformed_df.columns


# Pytest fixture for creating a SparkSession
@pytest.fixture(scope="session")
def spark_session(request):
    spark = SparkSession.builder.appName("pytest-pyspark").getOrCreate()

    # Set up cleanup after all tests are completed
    def teardown():
        spark.stop()

    request.addfinalizer(teardown)
    return spark
