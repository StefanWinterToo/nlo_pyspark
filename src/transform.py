from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def add_outcomes_to_bets_bronze(df) -> DataFrame:
    """
    returns a dataframe with the outcomes of the bets in the bronze table
    """

    df = df.withColumn("outcomes", transform(
    arrays_zip(col("legs"), col("markets")), lambda x: x
    ))
    return df

def trans_transactions(df):
    """
    returns a dataframe with the transactions of the bets in the bronze table
    """
    df = df.groupBy(col("sportsbook_id")).agg(collect_list(struct( [col(column) for column in df.columns if column != "sportsbook_id"] )).alias("transactions"))
    return df