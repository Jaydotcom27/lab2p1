#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal parking_violations_2023.csv  /part3/input/
echo "--- P2 ---"
start=$(date +%s)
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.5:7077 p3.py hdfs://10.128.0.5:9000/part3/input/ --conf spark.default.parallelism=2
end=$(date +%s)
echo "Elapsed Time P2: $(($end-$start)) seconds"
echo "--- P3 ---"
start=$(date +%s)
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.5:7077 p3.py hdfs://10.128.0.5:9000/part3/input/ --conf spark.default.parallelism=3
end=$(date +%s)
echo "Elapsed Time P3: $(($end-$start)) seconds"
echo "--- P4 ---"
start=$(date +%s)
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.5:7077 p3.py hdfs://10.128.0.5:9000/part3/input/ --conf spark.default.parallelism=4
end=$(date +%s)
echo "Elapsed Time P4: $(($end-$start)) seconds"
echo "--- P5 ---"
start=$(date +%s)
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.5:7077 p3.py hdfs://10.128.0.5:9000/part3/input/ --conf spark.default.parallelism=5
end=$(date +%s)
echo "Elapsed Time P5: $(($end-$start)) seconds"
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/output/
