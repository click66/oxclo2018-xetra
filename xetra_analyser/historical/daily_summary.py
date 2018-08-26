from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import *


def run_job(trades: DataFrame):
    """
    Generates daily summaries of provided trade data. Returns a DataFrame with the following columns:
     - Date
     - Number of Trades
    :param data: DataFrame
    :return: DataFrame
    """

    keyed_by_data = trades.rdd\
        .map(lambda row: (row.Date, row))\
        .reduceByKey(lambda a, b: Row(NumberOfTrades=a.NumberOfTrades+b.NumberOfTrades))

    keyed_by_data.toDF().show()

    return keyed_by_data.toDF()
