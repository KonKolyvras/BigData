from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# 1. Ορισμός του Schema
schema = StructType() \
    .add("name", StringType()) \
    .add("dn", IntegerType()) \
    .add("orig", StringType()) \
    .add("dest", StringType()) \
    .add("t", DoubleType()) \
    .add("link", StringType()) \
    .add("x", DoubleType()) \
    .add("s", DoubleType()) \
    .add("v", DoubleType())

# 2. Δημιουργία Spark Session
spark = (
    SparkSession.builder
    .appName("UXSIM-Consumer")
    .master("spark://spark-master:7077")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
    .getOrCreate()
)

# 3. Σύνδεση στον Redpanda (Kafka-compatible)
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "vehicle_positions")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parsing του JSON και Μετασχηματισμός
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 5. Υπολογισμός Στατιστικών ανά Ακμή (link) και Χρόνο (t)
stats = (
    parsed.groupBy("t", "link")
    .agg(
        count("*").alias("vcount"),
        avg("v").alias("vspeed")
    )
    .withColumnRenamed("t", "time")
)

# 6α. Αποθήκευση στη MongoDB των αρχικών δεδομενων
query_raw_mongo = (
    parsed.writeStream
    .format("mongodb")
    .option("checkpointLocation", "/tmp/checkpoint/raw")
    .option("database", "traffic")
    .option("collection", "raw_data")
    .outputMode("append")
    .start()
)

# 6β. Αποθήκευση στη MongoDB των επεξεργασμενων δεδομενων
# Το MongoDB connector δεν υποστηριζει outputMode("update") απευθειας.
# Χρησιμοποιουμε foreachBatch που γραφει καθε micro-batch σαν append.
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

# 7. Προβολή στην κονσόλα για debugging
query_console = (
    stats.writeStream
    .format("console")
    .option("checkpointLocation", "/tmp/checkpoint/console")
    .outputMode("update")
    .start()
)

# Αναμονή για τον τερματισμό όλων των queries
spark.streams.awaitAnyTermination()
