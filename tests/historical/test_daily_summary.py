from pyspark.sql import DataFrame, SQLContext
from historical.daily_summary import run_job


def provide_test_dataframe(spark, data_dir):
    return SQLContext(spark).read.csv("file://" + data_dir + "/multiple_days.csv", header="true", inferSchema="true")


def test_one_row_per_day(spark, data_dir):
    data = provide_test_dataframe(spark, data_dir)

    result = run_job(data)
    assert type(result) is DataFrame
    assert result.count() is 3
