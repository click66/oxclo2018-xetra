import os

SPARK_CONFIG = {
    'host': os.environ['MASTER_HOST'],
    'port': 7077
}

HDFS_CONFIG = {
    'host': 'ec2-34-248-160-205.eu-west-1.compute.amazonaws.com',
    'port': 9000,
    'path': 'user/ubuntu/xetra/*'
}
