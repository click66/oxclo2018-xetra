from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import *


def run_job(trades: DataFrame):
    """
    Generates daily summaries of provided trade data. Returns a DataFrame with the following columns:
     - Date
     - NumberOfTrades
    :param trades: DataFrame
    :return: DataFrame
    """

    def to_tuple(row: Row):
        return row.Date, row

    def to_row(tuple):
        return tuple[1]

    def reducer(accum, row):
        accum = Row(
            Date=accum.Date,
            NumberOfTrades=(accum.NumberOfTrades + row.NumberOfTrades)
        )
        return accum

    return trades.rdd.map(to_tuple).reduceByKey(reducer).map(to_row).toDF()
