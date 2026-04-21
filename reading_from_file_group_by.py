from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    print("Creating Spark Session...")
    spark = SparkSession.builder \
        .appName("Streaming Application") \
        .config("spark.sql.shuffle.partitions", 3) \
        .master("local[2]") \
        .getOrCreate()
    
    
    orders_schema = "order_id long, order_date date, order_customer_id long, order_status string"
    orders_df = spark.readStream \
        .format("json") \
        .schema(orders_schema) \
        .option("path", "data/inputdir") \
        .load()
        
    orders_df.createGlobalTempView("orders")
    agg_orders = spark.sql("SELECT order_status, COUNT(*) as count FROM orders GROUP BY order_status")
    
    
    query = agg_orders.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "checkpointdir1") \
        .start()
    
    query.awaitTermination()