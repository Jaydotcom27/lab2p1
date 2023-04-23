#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part2/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part2/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal parking_violations_2023_2.csv /part2/input/
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.5:7077 p2.py hdfs://10.128.0.5:9000/part2/input/