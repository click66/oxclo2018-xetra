FROM ubuntu:18.04

RUN apt-get update -y && apt-get upgrade -y

# Install python
RUN apt-get install -y curl wget openjdk-8-jdk-headless python

# Download and extract spark
RUN mkdir /usr/lib/spark; curl -s http://apache.mirror.anlx.net/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz | tar -xz -C /usr/lib/spark/ --strip-components 1

ENTRYPOINT /usr/lib/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
