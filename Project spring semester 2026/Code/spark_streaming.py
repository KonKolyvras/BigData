from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Ορισμός δομής των JSON μηνυμάτων — λέει στο Spark τι πεδία και τύπους έχει το κάθε record
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

# Σύνδεση με το Spark cluster και ρύθμιση MongoDB για εγγραφή αποτελεσμάτων
spark = (
    SparkSession.builder
    .appName("UXSIM-Spark-Streaming")
    .master("spark://spark-master:7077")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Σύνδεση στο Redpanda topic και έναρξη ανάγνωσης μηνυμάτων σε πραγματικό χρόνο
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "vehicle_positions")
    .option("startingOffsets", "latest")
    .load()
)

# Μετατροπή binary Kafka value → JSON string → DataFrame με ξεχωριστές στήλες
parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# Υπολογισμός στατιστικών ανά ακμή: vcount=πλήθος οχημάτων, vspeed=μέση ταχύτητα
stats = (
    parsed.groupBy("t", "link")
    .agg(
        count("*").alias("vcount"),
        avg("v").alias("vspeed")
    )
    .withColumnRenamed("t", "time")
)

# Εγγραφή raw δεδομένων (κάθε όχημα ξεχωριστά) στο MongoDB — append: δεν σβήνει παλιά
query_raw_mongo = (
    parsed.writeStream
    .format("mongodb")
    .option("checkpointLocation", "/tmp/checkpoint/raw")
    .option("database", "traffic")
    .option("collection", "raw_data")
    .outputMode("append")
    .start()
)

# foreachBatch: workaround γιατί MongoDB δεν υποστηρίζει update mode με aggregations απευθείας
def write_stats_to_mongo(batch_df, batch_id):
    batch_df.write \
        .format("mongodb") \
        .option("database", "traffic") \
        .option("collection", "stats") \
        .mode("append") \
        .save()

# Εγγραφή στατιστικών στο MongoDB — update: παράγει μόνο αλλαγμένες γραμμές
query_stats_mongo = (
    stats.writeStream
    .option("checkpointLocation", "/tmp/checkpoint/stats")
    .outputMode("update")
    .foreachBatch(write_stats_to_mongo)
    .start()
)

# Εκτύπωση στατιστικών στο terminal για debugging και screenshots
query_console = (
    stats.writeStream
    .format("console")
    .option("checkpointLocation", "/tmp/checkpoint/console")
    .outputMode("update")
    .start()
)

print("Spark Structured Streaming job successfully started. Waiting for stream data...")

# Κρατά το script ζωντανό — χωρίς αυτό τερματίζει αμέσως
spark.streams.awaitAnyTermination()
