-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Delta Lake Multi-Hop Pipeline with Streaming
-- MAGIC
-- MAGIC This notebook demonstrates how to use streaming for real-time data ingestion and transformation in a Delta Lake multi-hop pipeline.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Setting Up the Environment

-- COMMAND ----------

-- MAGIC %sql
-- Set directories for the pipeline
SET bronze_dir = 'dbfs:/mnt/demo/bronze';
SET silver_dir = 'dbfs:/mnt/demo/silver';
SET gold_dir = 'dbfs:/mnt/demo/gold';
SET checkpoint_dir = 'dbfs:/mnt/demo/checkpoints';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Bronze Layer: Streaming Data Ingestion

-- COMMAND ----------

-- MAGIC %sql
-- Create Bronze Delta table for raw streaming data
DROP TABLE IF EXISTS orders_bronze;

CREATE TABLE orders_bronze (
    order_id INT,
    customer_id INT,
    book_id INT,
    quantity INT,
    order_timestamp LONG,
    arrival_time TIMESTAMP,
    source_file STRING
) USING DELTA
LOCATION '${bronze_dir}';

-- COMMAND ----------

-- MAGIC %python
# Simulate raw data for streaming
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def generate_raw_data(directory, file_id):
    raw_data = [
        (file_id * 10 + 1, 101, 1, 2, int(datetime(2025, 1, 1).timestamp())),
        (file_id * 10 + 2, 102, 2, 1, int(datetime(2025, 1, 2).timestamp())),
        (file_id * 10 + 3, 103, 3, 3, int(datetime(2025, 1, 3).timestamp())),
    ]
    columns = ["order_id", "customer_id", "book_id", "quantity", "order_timestamp"]
    df = spark.createDataFrame(raw_data, columns)
    df.write.mode("overwrite").parquet(f"dbfs:/mnt/demo/raw/orders_{file_id}.parquet")

# Generate an initial batch of raw data
generate_raw_data("dbfs:/mnt/demo/raw", 1)

-- COMMAND ----------

-- MAGIC %python
# Configure a streaming source for the Bronze layer using Auto Loader
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")  # Source format
    .option("cloudFiles.schemaLocation", "${checkpoint_dir}/bronze/schema")  # Schema checkpoint
    .load("dbfs:/mnt/demo/raw")  # Source directory
    .withColumn("arrival_time", current_timestamp())  # Add metadata
    .withColumn("source_file", input_file_name())  # Add metadata
)

# Write the stream into the Bronze Delta table
bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")  # Append-only mode for raw data
    .option("checkpointLocation", "${checkpoint_dir}/bronze/checkpoint")  # Checkpoint directory
    .toTable("orders_bronze")
)

print("Bronze streaming query started.")

-- COMMAND ----------

-- MAGIC %sql
-- Verify Bronze Table
SELECT * FROM orders_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. Silver Layer: Data Enrichment

-- COMMAND ----------

-- MAGIC %sql
-- Create Silver Delta table for enriched data
DROP TABLE IF EXISTS orders_silver;

CREATE TABLE orders_silver (
    order_id INT,
    customer_id INT,
    customer_name STRING,
    book_id INT,
    book_title STRING,
    quantity INT,
    order_timestamp TIMESTAMP
) USING DELTA
LOCATION '${silver_dir}';

-- COMMAND ----------

-- MAGIC %python
# Static lookup tables for customers and books
customer_data = [
    (101, "John Doe"),
    (102, "Jane Smith"),
    (103, "Bob Johnson"),
]
book_data = [
    (1, "Databricks for Beginners"),
    (2, "Advanced Spark Techniques"),
    (3, "Delta Lake Mastery"),
]

customer_df = spark.createDataFrame(customer_data, ["customer_id", "customer_name"])
book_df = spark.createDataFrame(book_data, ["book_id", "book_title"])

customer_df.write.format("json").save("dbfs:/mnt/demo/lookup/customers")
book_df.write.format("json").save("dbfs:/mnt/demo/lookup/books")

-- COMMAND ----------

-- MAGIC %python
# Create streaming transformation for Silver layer
bronze_df = spark.readStream.table("orders_bronze")

customers_df = spark.read.format("json").load("dbfs:/mnt/demo/lookup/customers")
books_df = spark.read.format("json").load("dbfs:/mnt/demo/lookup/books")

silver_stream = (
    bronze_df
    .join(customers_df, "customer_id")  # Join with customers
    .join(books_df, "book_id")  # Join with books
    .select(
        "order_id",
        "customer_id",
        "customer_name",
        "book_id",
        "book_title",
        "quantity",
        from_unixtime("order_timestamp").alias("order_timestamp")
    )
)

# Write the stream into the Silver Delta table
silver_query = (
    silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "${checkpoint_dir}/silver/checkpoint")
    .toTable("orders_silver")
)

print("Silver streaming query started.")

-- COMMAND ----------

-- MAGIC %sql
-- Verify Silver Table
SELECT * FROM orders_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 4. Gold Layer: Aggregated Data

-- COMMAND ----------

-- MAGIC %sql
-- Create Gold Delta table for aggregated data
DROP TABLE IF EXISTS daily_customer_books;

CREATE TABLE daily_customer_books (
    customer_id INT,
    customer_name STRING,
    order_date DATE,
    books_count INT
) USING DELTA
LOCATION '${gold_dir}';

-- COMMAND ----------

-- MAGIC %python
# Streaming aggregation for Gold layer
silver_df = spark.readStream.table("orders_silver")

gold_stream = (
    silver_df
    .groupBy(
        "customer_id",
        "customer_name",
        date_trunc("DAY", "order_timestamp").alias("order_date")
    )
    .agg(sum("quantity").alias("books_count"))
)

# Write the aggregated stream into the Gold Delta table
gold_query = (
    gold_stream.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", "${checkpoint_dir}/gold/checkpoint")
    .toTable("daily_customer_books")
)

print("Gold streaming query started.")

-- COMMAND ----------

-- MAGIC %sql
-- Verify Gold Table
SELECT * FROM daily_customer_books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 5. Testing and Simulating New Data

-- COMMAND ----------

-- MAGIC %python
# Simulate new data arriving in the source directory
generate_raw_data("dbfs:/mnt/demo/raw", 2)

-- COMMAND ----------

-- MAGIC %sql
-- Verify updated counts in the Gold table
SELECT COUNT(*) FROM daily_customer_books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 6. Stop All Active Streams

-- COMMAND ----------

-- MAGIC %python
# Stop all active streams
for stream in spark.streams.active:
    print(f"Stopping stream: {stream.id}")
    stream.stop()