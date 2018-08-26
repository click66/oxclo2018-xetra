from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def filter_hours(dataframe: DataFrame):
    """
    Given a validated dataframe, filter the dataframe
    to include only 07:00 - 17:00 GMT (core trading hours
    of winter and summer time).
    :param dataframe:DataFrame
    :return: DataFrame
    """

    return dataframe.withColumn(
        'hour',
        hour(to_timestamp(concat_ws(' ', date_format(col('Date'), 'yyyy-MM-dd'), col('Time')), 'yyyy-MM-dd HH:mm'))
    )\
        .filter((col('hour') >= lit(7)) & (col('hour') <= lit(17)))\
        .drop('hour')
