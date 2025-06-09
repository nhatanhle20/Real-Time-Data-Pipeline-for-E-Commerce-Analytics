from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, sum, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType
import psycopg2

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
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss","false") \
    .load()

# Convert value column from bytes to JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Aggregate: total quantity sold per category per batch
agg_df = json_df \
    .withColumn("quantity", col("quantity").cast("integer")) \
    .groupBy("category") \
    .agg(sum("quantity").alias("batch_quantity"))

# Write to both batch details and summary tables
def write_batch_and_summary(batch_df, batch_id):
    categories_data = batch_df.collect()
    
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cursor = conn.cursor()
    
    for row in categories_data:
        category = row['category']
        quantity = row['batch_quantity']
        
        # Insert into batch detail table
        cursor.execute("""
            INSERT INTO batch_details (batch_id, category, quantity, processed_at) 
            VALUES (%s, %s, %s, NOW())
        """, (batch_id, category, quantity))
        
        # Update summary table (maintains all categories persistently)
        cursor.execute("""
            INSERT INTO sales_total (category, total_quantity, last_updated) 
            VALUES (%s, %s, NOW())
            ON CONFLICT (category) 
            DO UPDATE SET 
                total_quantity = sales_total.total_quantity + EXCLUDED.total_quantity,
                last_updated = NOW()
        """, (category, quantity))
    
    conn.commit()
    conn.close()
    print(f"Batch {batch_id} written to both detail and summary tables")

# Create table if not exist
def create_tables_if_not_exist():
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cursor = conn.cursor()
    
    # Create batch_details table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS batch_details (
            id SERIAL PRIMARY KEY,
            batch_id BIGINT NOT NULL,
            category VARCHAR(255) NOT NULL,
            quantity INTEGER NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create sales_total table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_total (
            category VARCHAR(255) PRIMARY KEY,
            total_quantity INTEGER NOT NULL DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create indexes
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_batch_details_category ON batch_details(category)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_batch_details_batch_id ON batch_details(batch_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_sales_total_updated ON sales_total(last_updated)
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created successfully!")

# Call this function before starting your streaming query
create_tables_if_not_exist()

# Then continue with your existing streaming code...
query = agg_df.writeStream \
    .foreachBatch(write_batch_and_summary) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

query.awaitTermination()


# Use foreachBatch to process each micro-batch
query = agg_df.writeStream \
    .foreachBatch(write_batch_and_summary) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()
