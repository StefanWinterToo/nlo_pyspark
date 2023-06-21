import pyarrow.parquet as pq
import pyarrow as pa
import json

def load_data(bets_bronze, trans_bronze, load_path = './Case_1/bets_interview_completed.parquet', debug = 0) -> None:
    """
    Joins bets_bronze and trans_bronze.
    
    Parameters
    ----------
    bets_bronze: Spark DataFrame
        Bronze table containing bets data.
    trans_bronze: Spark DataFrame
        Bronze table containing transactions data.
    load_path: str
        Path to save the joined table.
    debug: int
        Prints dataframe
    Returns None
    """
    bets_silver = bets_bronze.join(trans_bronze, bets_bronze.sportsbook_id == trans_bronze.sportsbook_id, "left").drop(trans_bronze.sportsbook_id)
    bet_gold = bets_silver.select(["sportsbook_id", "account_id", "outcomes", "transactions"])
    #bet_gold = bet_gold.select(["sportsbook_id", "account_id", "transactions"])

    if debug == 1:
        bet_gold.show()
        bet_gold.printSchema()
    else:
        bet_gold = bet_gold.toPandas()
        bet_gold['transactions'] = bet_gold['transactions'].apply(json.dumps)
        bet_gold['outcomes'] = bet_gold['outcomes'].apply(json.dumps)
        table = pa.Table.from_pandas(bet_gold)
        pq.write_table(table, load_path)

        # pq.write_table(pa.Table.from_pandas(bet_gold.toPandas()), load_path) -> This does not allow to write array/structs

    return None
