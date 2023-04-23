#!/usr/bin/python
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
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
COLORS = ["Black", "BLK", "BK", "BK.", "BLAC", "BK/", "BCK", "BLK.", "B LAC", "BC"]

if __name__ == "__main__":
    # Get file path from command-line argument
    file_path = str(sys.argv[1]).strip()

    # Create a Spark session
    spark = SparkSession.builder.appName("BlackCarTicket").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Read CSV file with predefined schema and select required columns
    violations = spark.read.csv(file_path, header=True, schema=schema)
    violations = violations.select("Vehicle Color", "Street Code1", "Street Code2", "Street Code3").na.drop()

    # Count the number of black car violations on specified streets
    black_car_violations = violations.filter(
        violations["Vehicle Color"].isin(COLORS) &
        (
            violations["Street Code1"].isin(STREETS) |
            violations["Street Code2"].isin(STREETS) |
            violations["Street Code3"].isin(STREETS)
        )
    )
    yes_count = black_car_violations.count()

    # Count the total number of violations
    total_count = violations.select("Vehicle Color").count()

    # Calculate the probability of a black car parking illegally on specified streets
    final_probability = yes_count / total_count

    # Print the result
    print("The probability of a black car parking illegally is:", final_probability)
        
   