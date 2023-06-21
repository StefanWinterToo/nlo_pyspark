import pyspark
from pyspark.sql import DataFrame
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from extract import *
from transform import *
from load import *

def main():
    spark = SparkSession.builder.appName("case").getOrCreate()
    
    # Load Data
    try:
        bets_bronze = extract_bets(spark)
        trans_bronze = extract_trans(spark)
    except Exception as e:
        print("Couldn't load sources: ", str(e))
    
    # Process Data
    try:
        bets_bronze = add_outcomes_to_bets_bronze(bets_bronze)
        trans_bronze = trans_transactions(trans_bronze)
    except Exception as e:
        print("Couldn't process data: ", str(e))

    # Load Data
    try:
        load_data(bets_bronze, trans_bronze)
    except Exception as e:
        print("Couldn't write data: ", e)


if __name__ == "__main__":
    main()