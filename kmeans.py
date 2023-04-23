# import sys
# from pyspark.sql.functions import when, round
# from pyspark.ml.feature import VectorAssembler
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType
# from pyspark.ml.clustering import KMeans
# from pyspark.ml.evaluation import ClusteringEvaluator


# if __name__ == "__main__":
#     spark = SparkSession.builder\
#         .appName("kmeans").getOrCreate()
#     spark.sparkContext.setLogLevel("ERROR")

#     shotData = spark.read.csv(sys.argv[1], inferSchema=True, header=True).select("SHOT_DIST", "SHOT_RESULT", "CLOSE_DEF_DIST", "SHOT_CLOCK", "player_name", "player_id")        
    
#     avgShotData = shotData.groupBy('player_id').avg("SHOT_DIST", "CLOSE_DEF_DIST", "SHOT_CLOCK")
#     trainData = avgShotData.drop('player_id')
    
#     assembler = VectorAssembler(inputCols=trainData.columns, outputCol='features')
    
#     trainData = assembler.transform(trainData)
    
#     players = ['201939', '201935', '2544', '101108']
#     testData = avgShotData.filter(avgShotData.player_id.isin(players))
#     testData = assembler.transform(testData)

#     hitRate = shotData.withColumn('made', when(shotData.SHOT_RESULT == "made", 1).otherwise(0)).select('player_id', 'player_name', 'SHOT_RESULT', 'made')
#     hitRate = hitRate.filter(hitRate.player_id.isin(players))
#     hitRate = hitRate.groupBy('player_id', 'player_name').agg({'made':'sum', 'player_id':'count'})
#     hitRate = hitRate.withColumn('hitRate', round(hitRate['sum(made)'] / hitRate['count(player_id)'],4)*100)

#     kmeans = KMeans(k=4).setSeed(1)
#     model = kmeans.fit(trainData)

#     predictions = model.transform(testData)
#     predictions.show()

#     hitRate = hitRate.join(predictions.select('player_id', 'prediction'), 'player_id', 'inner')
#     hitRate.show()
#     centers = model.clusterCenters()
#     print("Cluster Centers: ")
#     for center in centers:
#         print(center)

#     spark.stop()


# Import necessary modules, including some specific functions instead of the whole module
import sys
from pyspark.sql.functions import when, round, avg
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Define a main function for better organization and testing
def main(players):
    # Create a SparkSession
    spark = SparkSession.builder\
        .appName("kmeans").getOrCreate()
    # Set the log level to ERROR to reduce console output
    spark.sparkContext.setLogLevel("ERROR")

    # Read in the NBA player shooting data from a CSV file and select relevant columns
    shot_data = spark.read.csv(sys.argv[1], inferSchema=True, header=True)\
        .select("SHOT_DIST", "SHOT_RESULT", "CLOSE_DEF_DIST", "SHOT_CLOCK", "player_name", "player_id")
    
    # Compute the average shooting distance, distance from defender, and shot clock time for each player and use to create a training data set
    avg_shot_data = shot_data.groupBy('player_id').agg(avg("SHOT_DIST").alias("avg_SHOT_DIST"), 
                                                       avg("CLOSE_DEF_DIST").alias("avg_CLOSE_DEF_DIST"), 
                                                       avg("SHOT_CLOCK").alias("avg_SHOT_CLOCK"))
    train_data = avg_shot_data.drop('player_id')
    
    # Assemble the features of the training data set into a single column named 'features'
    assembler = VectorAssembler(inputCols=train_data.columns, outputCol='features')
    train_data = assembler.transform(train_data)
    
    # Create a test data set by selecting the same features for a subset of players in the training data set
    test_data = avg_shot_data.filter(avg_shot_data.player_id.isin(players))
    test_data = assembler.transform(test_data)

    # Compute the hit rate for each player and use to create a summary data frame
    hit_rate = shot_data.withColumn('made', when(shot_data.SHOT_RESULT == "made", 1).otherwise(0))\
        .select('player_id', 'player_name', 'SHOT_RESULT', 'made')
    hit_rate = hit_rate.filter(hit_rate.player_id.isin(players))
    hit_rate = hit_rate.groupBy('player_id', 'player_name').agg({'made':'sum', 'player_id':'count'})
    hit_rate = hit_rate.withColumn('hitRate', round(hit_rate['sum(made)'] / hit_rate['count(player_id)'],4)*100)

    # Train a KMeans clustering model on the training data set with k=4 clusters and a seed of 1
    kmeans = KMeans(k=4, seed=1)
    model = kmeans.fit(train_data)

    # Use the test data set to make predictions using the trained model
    predictions = model.transform(test_data)

    # Join the hit rate data frame with the predictions data frame to add a new column containing the predicted cluster for each player
    hit_rate = hit_rate.join(predictions.select('player_id', 'prediction'), 'player_id', 'inner')

    # Print the cluster centers to the console
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    # Define the input file path and player IDs as variables
    players = ['201939', '201935', '2544', '101108']
    main(players)