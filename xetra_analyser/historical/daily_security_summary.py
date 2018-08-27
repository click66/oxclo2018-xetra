from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import asc, desc, first, last, max, min, sum, udf
from pyspark.sql.types import FloatType
from xetra_analyser.utils.ratio_of_change import calculate as calculate_roc


def run_job(trades: DataFrame):
    """
    Generates daily summaries of provided trade data grouped by security.
    Returns a DataFrame with the following columns:
     - Security
     - Date
     - TradedVolume
     - NumberOfTrades
     - StartPrice
     - EndPrice
     - HighPrice
     - LowPrice
     - Volatility
    DataFrame is ordered alphabetically by trade, then in chronological reverse order.
    :param trades: DataFrame
    :return: DataFrame
    """

    with_roc = trades.withColumn("ROC", udf(calculate_roc, FloatType())(trades.StartPrice, trades.EndPrice))

    grouped = with_roc.groupBy('Mnemonic', 'Date')\
        .agg(
            sum('TradedVolume').alias('TradedVolume'),
            sum('NumberOfTrades').alias('NumberOfTrades'),
            first('StartPrice').alias('StartPrice'),
            last('EndPrice').alias('EndPrice'),
            max('MaxPrice').alias('HighPrice'),
            min('MinPrice').alias('LowPrice'),
            sum('ROC').alias('Volatility'),
        )\
        .withColumnRenamed('Mnemonic', 'Security') \
        .orderBy(asc('Security'), desc('Date'))

    return grouped
