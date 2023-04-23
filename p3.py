from pyspark.sql import SparkSession
from operator import add
import sys

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("park_hour").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    violations = spark.read.csv(sys.argv[1], inferSchema=True, header=True)
    data = violations.select('Violation Time').na.drop().rdd
    hour = data.map(lambda x: x[0]).map(lambda x: (x[:2]+ x[-1], 1)).reduceByKey(add)
    for h in hour.sortBy(lambda x: x[1],ascending=False).take(25):
        print(h)
    spark.stop()
