from pyspark.sql import SQLContext
from pyspark.sql.functions import to_utc_timestamp,from_utc_timestamp,concat,col,date_format,lit


def main(spark_context):
    sql_context = SQLContext(spark_context)

    data = sql_context.read.csv(
        "file:///app/data/small.csv",
        header="true",
        inferSchema="true"
    )

    data = data.withColumn(
        "Timestamp",
        to_utc_timestamp(concat(date_format(col("Date"), "YYYY-MM-dd "), col("Time")), "GMT")
    )

    print data
