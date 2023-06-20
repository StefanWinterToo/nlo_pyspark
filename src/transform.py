from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def add_outcomes_to_bets_bronze(df):
    df = df.withColumn("outcomes", transform(
    arrays_zip(col("legs"), col("markets")), lambda x: x
    ))
    return df

def trans_transactions(df):
    df = df.groupBy(col("sportsbook_id")).agg(collect_list(struct( [col(column) for column in df.columns if column != "sportsbook_id"] )).alias("transactions"))
    return df