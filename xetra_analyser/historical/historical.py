from pyspark import SparkContext
from pyspark.sql import SQLContext

from utils.filter_hours import filter_hours


def main(spark_context: SparkContext):
    # Tasks
    # 1) Init the context
    # 2) Read the data
    # 3) Filter the data to the data on which we want to run analysis
    # 4) Run all the reports, outputting the results
    # 5) Exit.
    sql_context = SQLContext(spark_context)

    data = sql_context.read.csv(
        "file:///app/data/small.csv",
        header="true",
        inferSchema="true"
    )

    data = filter_hours(data)

    data.show()

    # data = data.withColumn(
    #     "Timestamp",
    #     to_utc_timestamp(concat(date_format(col("Date"), "YYYY-MM-dd "), col("Time")), "GMT")
    # )
    #
    # print(data)

