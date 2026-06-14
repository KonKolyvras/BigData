from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# 1. Definition of Schema for UXSIM vehicle position records
schema = (
    StructType()
    .add("name", StringType())
    .add("dn", IntegerType())
    .add("orig", StringType())
    .add("dest", StringType())
    .add("t", DoubleType())
    .add("link", StringType())
    .add("x", DoubleType())
    .add("s", DoubleType())
    .add("v", DoubleType())
)

# 2. Initialize Spark Session configured with MongoDB
spark = (
    SparkSession.builder
    .appName("UXSIM-Spark-Streaming")
    .master("spark://spark-master:7077")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 3. Connect to Redpanda Broker (Kafka wire protocol compatible)
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "vehicle_positions")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parse the binary JSON value to fields according to the schema
parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# 5. Compute Link Statistics (vcount: count of vehicles, vspeed: average speed)
stats = (
    parsed.groupBy("t", "link")
    .agg(
        count("*").alias("vcount"),
        avg("v").alias("vspeed")
    )
    .withColumnRenamed("t", "time")
)

# 6a. Write raw records directly to MongoDB (Collection: raw_data, Database: traffic)
query_raw_mongo = (
    parsed.writeStream
    .format("mongodb")
    .option("checkpointLocation", "/tmp/checkpoint/raw")
    .option("database", "traffic")
    .option("collection", "raw_data")
    .outputMode("append")
    .start()
)

# 6b. Write aggregated statistics to MongoDB using foreachBatch (Collection: stats, Database: traffic)
# The MongoDB connector does not support direct Structured Streaming with outputMode("update").
# We write each micro-batch as append mode via foreachBatch.
def write_stats_to_mongo(batch_df, batch_id):
    batch_df.write \
        .format("mongodb") \
        .option("database", "traffic") \
        .option("collection", "stats") \
        .mode("append") \
        .save()

query_stats_mongo = (
    stats.writeStream
    .option("checkpointLocation", "/tmp/checkpoint/stats")
    .outputMode("update")
    .foreachBatch(write_stats_to_mongo)
    .start()
)

# 7. Print statistics to console for debugging
query_console = (
    stats.writeStream
    .format("console")
    .option("checkpointLocation", "/tmp/checkpoint/console")
    .outputMode("update")
    .start()
)

print("Spark Structured Streaming job successfully started. Waiting for stream data...")

# Wait for termination of all streaming queries
spark.streams.awaitAnyTermination()
