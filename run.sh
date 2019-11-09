#!/bin/bash

INPUT="hdfs://nashville:30841/cs435/pagerank/pract.txt"
TITLE="hdfs://nashville:30841/cs435/pagerank/titles.txt"
OUTPUT="hdfs://nashville:30841/cs435/pagerank/IdealResults2"
TAXATION="yeet"

sbt package
$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
$HADOOP_HOME/bin/hadoop fs -rm -r /cs435/pagerank/IdealResults2
$SPARK_HOME/bin/spark-submit --master spark://nashville:30860 --deploy-mode cluster --class IdealPR --supervise target/scala-2.11/sparkpagerank_2.11-1.0.jar $INPUT $TITLE $OUTPUT $TAXATION
