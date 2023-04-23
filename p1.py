import sys
from pyspark.sql.functions import when, round, avg
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans

def main(players):
    spark = SparkSession.builder\
        .appName("kmeans").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # selecting relevant cols from the csv 
    shot_data = spark.read.csv(sys.argv[1], inferSchema=True, header=True)\
        .select("SHOT_DIST", "SHOT_RESULT", "CLOSE_DEF_DIST", "SHOT_CLOCK", "player_name", "player_id")
    
    # getting averages using pyspark sql helper
    avg_shot_data = shot_data.groupBy('player_id').agg(avg("SHOT_DIST").alias("avg_SHOT_DIST"), 
                                                       avg("CLOSE_DEF_DIST").alias("avg_CLOSE_DEF_DIST"), 
                                                       avg("SHOT_CLOCK").alias("avg_SHOT_CLOCK"))
    train_data = avg_shot_data.drop('player_id')

    # PySpark model building
    assembler = VectorAssembler(inputCols=train_data.columns, outputCol='features')
    train_data = assembler.transform(train_data)
    test_data = avg_shot_data.filter(avg_shot_data.player_id.isin(players))
    test_data = assembler.transform(test_data)

    # Generating hit rate
    hit_rate = shot_data.withColumn('made', when(shot_data.SHOT_RESULT == "made", 1).otherwise(0))\
        .select('player_id', 'player_name', 'SHOT_RESULT', 'made')
    hit_rate = hit_rate.filter(hit_rate.player_id.isin(players))
    hit_rate = hit_rate.groupBy('player_id', 'player_name').agg({'made':'sum', 'player_id':'count'})
    hit_rate = hit_rate.withColumn('hitRate', round(hit_rate['sum(made)'] / hit_rate['count(player_id)'],4)*100)

    # Kmeans training with 4 clusters
    kmeans = KMeans(k=4, seed=1)
    model = kmeans.fit(train_data)

    # Model predictions
    predictions = model.transform(test_data)
    predictions.show()
    hit_rate = hit_rate.join(predictions.select('player_id', 'prediction'), 'player_id', 'inner')
    hit_rate.show()
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)
    spark.stop()

if __name__ == "__main__":
    # These are the players to be analyzed
    players = ['201939', '201935', '2544', '101108']
    main(players)