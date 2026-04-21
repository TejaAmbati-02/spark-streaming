from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    print("Creating Spark Session...")
    spark = SparkSession.builder \
        .appName("Streaming Application") \
        .config("spark.sql.shuffle.partitions", 3) \
        .master("local[2]") \
        .getOrCreate()
    
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9990) \
        .load()
        
    words = lines.select(explode(split(lines.value, " ")).alias("word"))
    word_counts = words.groupBy("word").count()
    
    

    query = word_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "checkpointdir") \
        .start()
    
    query.awaitTermination()