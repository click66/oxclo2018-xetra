from pyspark import SparkContext
from pyspark.sql import SQLContext

from xetra_analyser.utils.log import log
from xetra_analyser.utils.filter_hours import filter_hours


def main(spark_context: SparkContext):
    # Connect to Spark
    sql_context = SQLContext(spark_context)

    # Read the data from source
    data = sql_context.read.csv(
        "file:///app/data/dbs/*",
        header="true",
        inferSchema="true"
    )
    log("Read " + str(data.count()) + " rows from source")

    # Filter the data to hours
    data = filter_hours(data)
    log("Filtered data to " + str(data.count()) + " rows")

    data.show()
