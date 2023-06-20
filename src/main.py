import pyspark
from pyspark.sql import DataFrame
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from extract import *
from transform import *
import pyarrow.parquet as pq


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

    bets_silver = bets_bronze.join(trans_bronze, bets_bronze.sportsbook_id == trans_bronze.sportsbook_id, "left").drop(trans_bronze.sportsbook_id)
    bet_gold = bets_silver.select(["sportsbook_id", "account_id", "outcomes", "transactions"])
    bet_gold.show()
    
    pq.write_table(bet_gold, './Case_1/bets_interview_completed.parquet')


if __name__ == "__main__":
    main()