from pyspark.sql import DataFrame, SQLContext
from utils.filter_hours import filter_hours


def provide_test_dataframe(spark, data_dir):
    return SQLContext(spark).read.csv("file://" + data_dir + "/every_hour.csv", header="true", inferSchema="true")


def test_filters_hours(spark, data_dir):
    testdata = provide_test_dataframe(spark, data_dir)
    assert type(testdata) is DataFrame

    assert testdata.count() == 24

    filtered = filter_hours(testdata)
    assert filtered.count() == 11


def test_filtering_hours_leaves_columns_unaffected(spark, data_dir):
    testdata = provide_test_dataframe(spark, data_dir)
    assert type(testdata) is DataFrame

    original_columns = testdata.columns

    filtered = filter_hours(testdata)
    assert filtered.columns == original_columns
