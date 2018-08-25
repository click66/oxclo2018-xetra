from pyspark import SparkConf, SparkContext
import config


def get_spark():
    conf = SparkConf()\
        .setAppName("XetraAnalysis_Tests")\
        .setMaster("spark://" + config.SPARK_CONFIG.get("host") + ":" + str(config.SPARK_CONFIG.get("port")))\
        .set("spark.ui.enabled", "false")

    return SparkContext(conf=conf)
