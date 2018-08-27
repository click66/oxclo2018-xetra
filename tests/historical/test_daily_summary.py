from pyspark.sql import DataFrame, Row, SQLContext
from xetra_analyser.historical.daily_summary import run_job


def provide_test_dataframe(spark, data_dir):
    return SQLContext(spark).read.csv("file://" + data_dir + "/multiple_days.csv", header="true", inferSchema="true")


def test_run_job(spark, data_dir):
    result = run_job(provide_test_dataframe(spark, data_dir))

    assert type(result) is DataFrame
    return result


def test_one_row_per_day(spark, data_dir):
    result = test_run_job(spark, data_dir)

    assert result.count() is 3


def test_has_date(spark, data_dir):
    result = test_run_job(spark, data_dir)

    assert "Date" in result.columns


def test_has_number_of_trades(spark, data_dir):
    result = test_run_job(spark, data_dir)

    assert "NumberOfTrades" in result.columns


def test_has_counted_correct_numbers_of_trades(spark, data_dir):
    result = test_run_job(spark, data_dir)

    expected = [Row(NumberOfTrades=5), Row(NumberOfTrades=5), Row(NumberOfTrades=6)]

    assert result.select('NumberOfTrades').collect() == expected
