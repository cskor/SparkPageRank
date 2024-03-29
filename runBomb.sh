#!/bin/bash

INPUT="hdfs://nashville:30841/cs435/pagerank/links/*"
TITLE="hdfs://nashville:30841/cs435/pagerank/titles/*"
OUTPUT="hdfs://nashville:30841/cs435/pagerank/SPAMFINAL"

sbt package
$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
$HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUT
$SPARK_HOME/bin/spark-submit --master spark://nashville:30860 --class wikiBomb --supervise target/scala-2.11/sparkpagerank_2.11-1.0.jar  $TITLE $INPUT $OUTPUT
