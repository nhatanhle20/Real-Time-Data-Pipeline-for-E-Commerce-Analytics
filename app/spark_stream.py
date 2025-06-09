from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, sum, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType
import psycopg2


# Define schema for incoming JSON data
schema = StructType() \
    .add("order_id", StringType()) \
    .add("user_id", StringType()) \
    .add("user_name", StringType()) \
    .add("user_email", StringType()) \
    .add("product_id", StringType()) \
    .add("category", StringType()) \
    .add("price", FloatType()) \
    .add("quantity", IntegerType()) \
    .add("payment_method", StringType()) \
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
    
    # Create total_sales table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS total_sales (
            category VARCHAR(255) PRIMARY KEY,
            total_quantity INTEGER NOT NULL DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR(255) PRIMARY KEY,
            user_name VARCHAR(255) NOT NULL,
            user_email VARCHAR(255) NOT NULL,
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
        CREATE INDEX IF NOT EXISTS idx_total_sales_updated ON total_sales(last_updated)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_users_email ON users(user_email)
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("All tables created successfully!")


# Function to write transaction data to databases (with data type parameter)
def write_to_database(batch_df, batch_id, data_type=None):
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cursor = conn.cursor()
    
    # Auto-detect data type if not specified
    if data_type is None:
        columns = batch_df.columns
        if 'batch_quantity' in columns and len(columns) == 2:
            data_type = 'category'
        elif 'user_id' in columns:
            data_type = 'user'
    
    if data_type == 'category':
        # Process category aggregation data
        categories_data = batch_df.collect()
        
        for row in categories_data:
            category = row['category']
            quantity = row['batch_quantity']
            
            cursor.execute("""
                INSERT INTO batch_details (batch_id, category, quantity, processed_at) 
                VALUES (%s, %s, %s, NOW())
            """, (batch_id, category, quantity))
            
            cursor.execute("""
                INSERT INTO total_sales (category, total_quantity, last_updated) 
                VALUES (%s, %s, NOW())
                ON CONFLICT (category) 
                DO UPDATE SET 
                    total_quantity = total_sales.total_quantity + EXCLUDED.total_quantity,
                    last_updated = NOW()
            """, (category, quantity))
        
        print(f"Batch {batch_id}: {len(categories_data)} categories processed")
    
    elif data_type == 'user':
        # Process user data
        users_data = batch_df.select("user_id", "user_name", "user_email").distinct().collect()
        
        for user_row in users_data:
            user_id = user_row['user_id']
            user_name = user_row['user_name']
            user_email = user_row['user_email']

            cursor.execute("""
                INSERT INTO users (user_id, user_name, user_email, last_updated) 
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (user_id) 
                DO UPDATE SET 
                    user_name = EXCLUDED.user_name,
                    user_email = EXCLUDED.user_email,
                    last_updated = NOW()
            """, (user_id, user_name, user_email))
        
        print(f"Batch {batch_id}: {len(users_data)} users processed")
    
    conn.commit()
    cursor.close()
    conn.close()


# Create required tables
create_tables_if_not_exist()


# Use lambda functions to pass different data types
query1 = agg_df.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_database(df, batch_id, 'category')) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-categories") \
    .start()

query2 = json_df.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_database(df, batch_id, 'user')) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-users") \
    .start()

query1.awaitTermination()