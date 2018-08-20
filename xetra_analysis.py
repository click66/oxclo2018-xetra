#!/usr/bin/env python2

from pyspark import SparkConf, SparkContext
from xetra_analyser.historical import historical

import config

if __name__ == '__main__':
    conf = SparkConf() \
        .setAppName("XetraAnalysis") \
        .setMaster("spark://" + config.SPARK_CONFIG.get("host") + ":" + str(config.SPARK_CONFIG.get("port")))

    historical.main(SparkContext(conf=conf))
