import pyspark
from pyspark.sql import DataFrame
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

def extract_bets(spark):
    df = spark.read.parquet("./Case_1/bets_v1/part-00000-4ea82782-6afd-434c-8fee-42264167ffe1-c000.snappy.parquet")    
    df.show()

def extract_trans(spark):
    df = spark.read.parquet("./Case_1/trans_v1/part-00000-6d83da89-6ef2-4edc-8446-7838dce4bd1d-c000.snappy.parquet")    
    df.show()

def main():
    spark = SparkSession.builder.master("local[1]").appName("case").getOrCreate()
    extract_bets(spark)
    extract_trans(spark)

main()