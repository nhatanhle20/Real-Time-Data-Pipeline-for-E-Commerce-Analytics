from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

# Define schema for incoming JSON data
schema = StructType() \
    .add("order_id", StringType()) \
    .add("user_id", StringType()) \
    .add("product_id", StringType()) \
    .add("category", StringType()) \
    .add("price", FloatType()) \
    .add("quantity", IntegerType()) \
    .add("payment_method", StringType()) \
    .add("order_status", StringType()) \
    .add("timestamp", StringType())

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceStreamProcessor") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ecommerce-transactions") \
    .load()

# Convert value column from bytes to JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Aggregate: total quantity sold per category
agg_df = json_df.groupBy("category").sum("quantity")

# Output to console for testing
agg_df.writeStream \
    .format("bigquery") \
    .option("table", "ecommerce-data-pipeline-2025.ecommerce_analytics.sales_by_category") \
    .option("checkpointLocation", "/tmp/bq-checkpoint") \
    .option("parentProject", "ecommerce-data-pipeline-2025") \
    .outputMode("complete") \
    .start() \
    .awaitTermination()
