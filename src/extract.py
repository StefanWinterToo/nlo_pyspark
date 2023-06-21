
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def extract_bets(spark, debug = 0) -> DataFrame:
    """
    Loads df_bets from parquet file

    Parameters
    ----------
    spark : SparkSession
        Spark Session
    debug : int, optional
        Debug level, by default 0

    Returns DataFrame
    """
    df_bets = spark.read.parquet("./Case_1/bets_v1/part-00000-4ea82782-6afd-434c-8fee-42264167ffe1-c000.snappy.parquet")
    # Cast some fields
    df_bets = df_bets.withColumn("account_id", col("account_id").cast('int'))
    df_bets = df_bets.withColumn("legs", transform(col("legs"), lambda x, i: x.price.withField("decimal", x.price.decimal.cast('float'))))
    
    if debug == 1:
        df_bets.show(truncate=0)
        df_bets.printSchema()
    
    return df_bets


def extract_trans(spark, debug = 0) -> DataFrame:
    """
    Loads df_trans from parquet file

    Parameters
    ----------
    spark : SparkSession
        Spark Session
    debug : int, optional
        Debug level, by default 0

    Returns DataFrame
    """
    df_trans = spark.read.parquet("./Case_1/trans_v1/part-00000-6d83da89-6ef2-4edc-8446-7838dce4bd1d-c000.snappy.parquet")    
    
    if debug == 1:
        df_trans.show(truncate=0)
        df_trans.printSchema()
    
    return df_trans