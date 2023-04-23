#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal parking_violations_2023.csv  /part3/input/
echo "--- P2 ---"
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.5:7077 p3.py hdfs://10.128.0.5:9000/part3/input/ --conf spark.default.parallelism=2
echo "--- P3 ---"
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.5:7077 p3.py hdfs://10.128.0.5:9000/part3/input/ --conf spark.default.parallelism=3
echo "--- P4 ---"
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.5:7077 p3.py hdfs://10.128.0.5:9000/part3/input/ --conf spark.default.parallelism=4
echo "--- P5 ---"
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.5:7077 p3.py hdfs://10.128.0.5:9000/part3/input/ --conf spark.default.parallelism=5
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/output/
