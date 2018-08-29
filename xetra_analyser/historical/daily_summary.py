from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import desc
from pyspark.sql.types import datetime


def run_job(trades: DataFrame):
    """
    Generates daily summaries of provided trade data. Returns a DataFrame with the following columns:
     - Date
     - NumberOfTrades
    DataFrame is ordered in chronological reverse order.
    :param trades: DataFrame
    :return: DataFrame
    """

    def make_row(date: datetime, number_of_trades: int):
        return Row(
            Date=date,
            NumberOfTrades=number_of_trades
        )

    def to_tuple(row: Row):
        return row.Date, row

    def to_row(tuple):
        return tuple[1]

    def reducer(accum, row):
        accum = make_row(
            accum.Date,
            accum.NumberOfTrades + row.NumberOfTrades
        )
        return accum

    return trades.rdd.map(to_tuple).reduceByKey(reducer).map(to_row).toDF()\
        .orderBy(desc('Date'))
