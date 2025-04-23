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

# Function to write each batch to PostgreSQL
def write_batch_to_postgres(batch_df, batch_id):
    # Write the batch to PostgreSQL
    if not batch_df.isEmpty():
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://host.docker.internal:5432/ecommerce_db") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "sales_summary") \
            .option("user", "ecommerce") \
            .option("password", "ecommerce123") \
            .mode("overwrite") \
            .save()
        print(f"Batch {batch_id} written to PostgreSQL")

# Use foreachBatch to process each micro-batch
query = agg_df.writeStream \
    .foreachBatch(write_batch_to_postgres) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start() \

# Wait for the streaming query to terminate
query.awaitTermination()