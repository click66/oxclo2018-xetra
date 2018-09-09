from pyspark import SparkContext
from pyspark.sql import SQLContext

from xetra_analyser.historical import daily_security_summary, daily_summary
from xetra_analyser.utils.log import log
from xetra_analyser.utils.filter_hours import filter_hours

import config


def main(spark_context: SparkContext):
    log('Started execution')

    # Connect to Spark
    sql_context = SQLContext(spark_context)
    log('Connected to Spark')

    # Read the data from source
    hdfs_config = config.HDFS_CONFIG
    source = "hdfs://" + hdfs_config.get("host") + ":" + str(hdfs_config.get("port")) + "/" + hdfs_config.get("path")

    data = sql_context.read.csv("file:///app/data/test_sample/*", header="true", inferSchema="true") # Test
    log("Read " + str(data.count()) + " rows from source")

    # Filter the data to hours
    data = filter_hours(data)
    log("Filtered data to " + str(data.count()) + " rows")

    # Run all reports
    output_dir = "output"
    daily_security_summary.run_job(data).show()
