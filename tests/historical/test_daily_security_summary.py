from pyspark.sql import DataFrame, Row, SQLContext
from xetra_analyser.historical.daily_security_summary import run_job


def provide_test_dataframe(spark, data_dir):
    return SQLContext(spark).read.csv("file://" + data_dir + "/multiple_days.csv", header="true", inferSchema="true")


def test_run_job(spark, data_dir):
    result = run_job(provide_test_dataframe(spark, data_dir))

    assert type(result) is DataFrame
    return result


def test_has_expected_columns(spark, data_dir):
    result = test_run_job(spark, data_dir)

    for column in [
        "Security",
        "Date",
        "TradedVolume",
        "NumberOfTrades",
        "StartPrice",
        "EndPrice",
        "HighPrice",
        "LowPrice",
        "Volatility"
    ]:
        assert column in result.columns


def test_creates_correct_rows(spark, data_dir):
    result = test_run_job(spark, data_dir)

    assert result.count() is 12

    for security in ["SII", "DBD", "MMM", "WHR"]:
        assert result[result.Security == security].count() is 3
