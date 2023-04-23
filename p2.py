#!/usr/bin/python
import sys
from pyspark.sql import SparkSession
# import pyspark.sql.functions as psf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# I was having trouble with letting the spark csv reader infer the schema so I had to build it manually
# This is a bit cluttery but doesn't affect performance or results 

schema = StructType([
    StructField("Summons Number", LongType(), True),
    StructField("Plate ID", StringType(), True),
    StructField("Registration State", StringType(), True),
    StructField("Plate Type", StringType(), True),
    StructField("Issue Date", StringType(), True),
    StructField("Violation Code", IntegerType(), True),
    StructField("Vehicle Body Type", StringType(), True),
    StructField("Vehicle Make", StringType(), True),
    StructField("Issuing Agency", StringType(), True),
    StructField("Street Code1", IntegerType(), True),
    StructField("Street Code2", IntegerType(), True),
    StructField("Street Code3", IntegerType(), True),
    StructField("Vehicle Expiration Date", StringType(), True),
    StructField("Violation Location", StringType(), True),
    StructField("Violation Precinct", IntegerType(), True),
    StructField("Issuer Precinct", IntegerType(), True),
    StructField("Issuer Code", StringType(), True),
    StructField("Issuer Command", StringType(), True),
    StructField("Issuer Squad", StringType(), True),
    StructField("Violation Time", StringType(), True),
    StructField("Time First Observed", IntegerType(), True),
    StructField("Violation County", StringType(), True),
    StructField("Violation In Front Of Or Opposite", StringType(), True),
    StructField("House Number", StringType(), True),
    StructField("Street Name", StringType(), True),
    StructField("Intersecting Street", StringType(), True),
    StructField("Date First Observed", StringType(), True),
    StructField("Law Section", StringType(), True),
    StructField("Sub Division", StringType(), True),
    StructField("Violation Legal Code", StringType(), True),
    StructField("Days Parking In Effect", StringType(), True),
    StructField("From Hours In Effect", StringType(), True),
    StructField("To Hours In Effect", StringType(), True),
    StructField("Vehicle Color", StringType(), True),
    StructField("Unregistered Vehicle?", StringType(), True),
    StructField("Vehicle Year", IntegerType(), True),
    StructField("Meter Number", StringType(), True),
    StructField("Feet From Curb", IntegerType(), True),
    StructField("Violation Post Code", StringType(), True),
    StructField("Violation Description", StringType(), True),
    StructField("No Standing or Stopping Violation", StringType(), True),
    StructField("Hydrant Violation", StringType(), True),
    StructField("Double Parking Violation", StringType(), True)
])

STREETS = ["34510", "10030", "34050"]
COLORS = ['BLAC', 'BK', 'BK/', 'BC', 'BCK', 'Black', 'BLK.', 'BK.', 'BLK', 'B LAC']

if __name__ == "__main__":
    file_path = str(sys.argv[1]).strip()
    spark = SparkSession.builder.appName("BlackCarTicket").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # selecting relevant cols from the csv 
    violations = spark.read.csv(file_path, header=True, schema=schema)
    violations = violations.select("Vehicle Color", "Street Code1", "Street Code2", "Street Code3").na.drop()

    # Finding the number of violations in the required streets and only for black cars
    black_car_violations = violations.filter(
        violations["Vehicle Color"].isin(COLORS) &
        (
            violations["Street Code1"].isin(STREETS) |
            violations["Street Code2"].isin(STREETS) |
            violations["Street Code3"].isin(STREETS)
        )
    )
    yes_count = black_car_violations.count()
    print(yes_count)
    total_count = violations.select("Vehicle Color").count()
    print(total_count)

    # Getting final probability (this is a rough estimate based on a portion of the data since the dataset is gigantic)
    final_probability = yes_count / total_count
    print("The probability of a black car parking illegally is:", final_probability)
        
   