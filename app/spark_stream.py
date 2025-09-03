from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, sum, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType
import psycopg2
import csv
import os


# Define schema for incoming JSON data with new address and shipping fields
schema = StructType() \
    .add("order_id", StringType()) \
    .add("user_id", StringType()) \
    .add("user_name", StringType()) \
    .add("user_email", StringType()) \
    .add("street", StringType()) \
    .add("city", StringType()) \
    .add("state", StringType()) \
    .add("postal_code", StringType()) \
    .add("country", StringType()) \
    .add("product_id", StringType()) \
    .add("category", StringType()) \
    .add("price", FloatType()) \
    .add("quantity", IntegerType()) \
    .add("shipping_method", StringType()) \
    .add("payment_method", StringType()) \
    .add("timestamp", StringType())


# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceStreamProcessor") \
    .getOrCreate()


# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce-transactions") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss","false") \
    .load()


# Convert value column from bytes to JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# Aggregate: total quantity sold per category per batch
category_agg_df = json_df \
    .withColumn("quantity", col("quantity").cast("integer")) \
    .groupBy("category") \
    .agg(sum("quantity").alias("batch_quantity"))


# Aggregate: total quantity sold per city per batch
city_agg_df = json_df \
    .withColumn("quantity", col("quantity").cast("integer")) \
    .groupBy("city", "state") \
    .agg(sum("quantity").alias("batch_quantity"))


# Aggregate: total quantity sold per payment method per batch
payment_agg_df = json_df \
    .withColumn("quantity", col("quantity").cast("integer")) \
    .groupBy("payment_method") \
    .agg(sum("quantity").alias("batch_quantity"))


# Aggregate: total quantity sold per shipping method per batch
shipping_agg_df = json_df \
    .withColumn("quantity", col("quantity").cast("integer")) \
    .groupBy("shipping_method") \
    .agg(sum("quantity").alias("batch_quantity"))


# Function to create required table if not exist
def create_tables_if_not_exist():
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cursor = conn.cursor()

    # Set session timezone to UTC+07:00
    cursor.execute("SET TIME ZONE '+07:00'")
    
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

    # Create users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR(255) PRIMARY KEY,
            user_name VARCHAR(255) NOT NULL,
            user_email VARCHAR(255) NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(255) PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create shipping_methods table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shipping_methods (
            id SERIAL PRIMARY KEY,
            shipping_method VARCHAR(255) NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # # Insert default shipping methods if not exists
    # cursor.execute("""
    #     INSERT INTO shipping_methods (shipping_method) VALUES
    #     ('Free'),
    #     ('Standard'),
    #     ('Fast Express'),
    #     ('Next Day Shipping')
    # """)

    # Create payment_methods table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS payment_methods (
            id SERIAL PRIMARY KEY,
            payment_method VARCHAR(255) NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # # Insert default payment methods if not exists
    # cursor.execute("""
    #     INSERT INTO payment_methods (payment_method) VALUES
    #     ('Credit Card'),
    #     ('PayPal'),
    #     ('COD'),
    #     ('Bank Transfer')
    # """)

    # Create transaction_data table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transaction_data (
            id SERIAL PRIMARY KEY,
            order_id VARCHAR(255) NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            street VARCHAR(500) NOT NULL,
            city VARCHAR(255) NOT NULL,
            state VARCHAR(255) NOT NULL,
            postal_code VARCHAR(20) NOT NULL,
            country VARCHAR(255) NOT NULL,
            product_id VARCHAR(255) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            quantity INTEGER NOT NULL,
            shipping_method VARCHAR(100) NOT NULL,
            payment_method VARCHAR(100) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        CREATE INDEX IF NOT EXISTS idx_users_email ON users(user_email)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_transaction_user_id ON transaction_data(user_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_transaction_timestamp ON transaction_data(timestamp)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_transaction_city ON transaction_data(city)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_transaction_state ON transaction_data(state)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_transaction_country ON transaction_data(country)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_transaction_shipping_method ON transaction_data(shipping_method)
    """)

    
    conn.commit()
    cursor.close()
    conn.close()
    print("All tables created successfully!")


# Function to write transaction data to databases (with data type parameter)
def write_to_database(batch_df, batch_id, data_type=None):
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cursor = conn.cursor()
    
    # Set session timezone to UTC+07:00
    cursor.execute("SET TIME ZONE '+07:00'")

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
        
        print(f"Batch {batch_id}: {len(categories_data)} sales per catagories processed")
    
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
    
    elif data_type == 'transaction':
        # Process complete transaction data with new address and shipping fields
        transactions_data = batch_df.collect()
        
        for transaction_row in transactions_data:
            order_id = transaction_row['order_id']
            user_id = transaction_row['user_id']
            street = transaction_row['street']
            city = transaction_row['city']
            state = transaction_row['state']
            postal_code = transaction_row['postal_code']
            country = transaction_row['country']
            product_id = transaction_row['product_id']
            price = transaction_row['price']
            quantity = transaction_row['quantity']
            shipping_method = transaction_row['shipping_method']
            payment_method = transaction_row['payment_method']
            timestamp = transaction_row['timestamp']

            cursor.execute("""
                INSERT INTO transaction_data (
                    order_id, user_id, street, city, state, 
                    postal_code, country, product_id, price, quantity, 
                    shipping_method, payment_method, timestamp, processed_at
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                order_id, user_id, street, city, state,
                postal_code, country, product_id, price, quantity,
                shipping_method, payment_method, timestamp
            ))
        
        print(f"Batch {batch_id}: {len(transactions_data)} transactions processed")
    
    elif data_type == 'address':
        # Process city aggregation data
        address_data = batch_df.collect()
        
        for address_row in address_data:
            city = address_row['city']
            state = address_row['state']
            quantity = address_row['batch_quantity']
            
            cursor.execute("""
                INSERT INTO sales_per_city (city, state, total_quantity, last_updated) 
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (city, state) 
                DO UPDATE SET 
                    total_quantity = sales_per_city.total_quantity + EXCLUDED.total_quantity,
                    last_updated = NOW()
            """, (city, state, quantity))
        
        print(f"Batch {batch_id}: {len(address_data)} sales per cities processed")


    conn.commit()
    cursor.close()
    conn.close()


def import_us_coordinates():
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS us_coordinates (
            city VARCHAR(255) NOT NULL,
            state VARCHAR(255) NOT NULL,
            latitude DECIMAL(10, 8) NOT NULL,
            longitude DECIMAL(11, 8) NOT NULL,
            PRIMARY KEY (city, state)
        )
    """)

    csv_path = os.path.join(os.path.dirname(__file__), "data", "us_coordinates.csv")

    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows_inserted = 0

        for row in reader:
            city = row['city']
            state = row['state']
            latitude = float(row['latitude'])
            longitude = float(row['longitude'])

            cursor.execute("""
                INSERT INTO us_coordinates (city, state, latitude, longitude)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (city, state) DO NOTHING
            """, (city, state, latitude, longitude))

            rows_inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Imported {rows_inserted} rows into us_coordinates table")

# Import coordinates only if the table doesn't exist
def us_coordinates_table_exists():
    conn = psycopg2.connect("postgresql://ecommerce:ecommerce123@host.docker.internal:5432/ecommerce_db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'us_coordinates'
        )
    """)
    exists = cursor.fetchone()[0]

    cursor.close()
    conn.close()
    return exists


# Create required tables
create_tables_if_not_exist()

# Import coordinates from CSV if not already existed
if not us_coordinates_table_exists():
    import_us_coordinates()
else:
    print("us_coordinates table already exists — skipping import.")


# Use lambda functions to pass different data types

# Process category aggregations
query1 = category_agg_df.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_database(df, batch_id, 'category')) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-categories") \
    .start()


# Process user data
query2 = json_df.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_database(df, batch_id, 'user')) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-users") \
    .start()


# Process complete transaction data
query3 = json_df.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_database(df, batch_id, 'transaction')) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-transactions") \
    .start()


# Process complete transaction data
query4 = city_agg_df.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_database(df, batch_id, 'address')) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-cities") \
    .start()


# Process payment method aggregations
query5 = payment_agg_df.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_database(df, batch_id, 'payment')) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-payments") \
    .start()


# Process shipping method aggregations
query6 = shipping_agg_df.writeStream \
    .foreachBatch(lambda df, batch_id: write_to_database(df, batch_id, 'shipping')) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-shipping") \
    .start()


query1.awaitTermination(60)