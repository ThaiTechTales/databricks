-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Delta Lake Multi-Hop Pipeline with Streaming
-- MAGIC
-- MAGIC This notebook demonstrates how to build a Delta Lake pipeline with real-time streaming for Bronze, Silver, and Gold layers.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Create Bronze Delta Table for Raw Streaming Data

-- COMMAND ----------

-- Drop the table if it exists
DROP TABLE IF EXISTS orders_bronze;

-- Create a managed Bronze Delta table
CREATE TABLE orders_bronze (
    order_id INT,
    customer_id INT,
    book_id INT,
    quantity INT,
    order_timestamp LONG,
    arrival_time TIMESTAMP,
    source_file STRING
) USING DELTA;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Simulate raw data for streaming
-- MAGIC from datetime import datetime
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, LongType
-- MAGIC
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC
-- MAGIC # Define the schema explicitly
-- MAGIC raw_data_schema = StructType([
-- MAGIC     StructField("order_id", IntegerType(), True),
-- MAGIC     StructField("customer_id", IntegerType(), True),
-- MAGIC     StructField("book_id", IntegerType(), True),
-- MAGIC     StructField("quantity", IntegerType(), True),
-- MAGIC     StructField("order_timestamp", LongType(), True),  # Use LongType for timestamp
-- MAGIC ])
-- MAGIC
-- MAGIC # Function to generate raw data
-- MAGIC def generate_raw_data(directory, file_id):
-- MAGIC     # Create raw data
-- MAGIC     raw_data = [
-- MAGIC         (file_id * 10 + 1, 101, 1, 2, int(datetime(2025, 1, 1).timestamp())),
-- MAGIC         (file_id * 10 + 2, 102, 2, 1, int(datetime(2025, 1, 2).timestamp())),
-- MAGIC         (file_id * 10 + 3, 103, 3, 3, int(datetime(2025, 1, 3).timestamp())),
-- MAGIC     ]
-- MAGIC     # Apply schema to ensure consistency
-- MAGIC     df = spark.createDataFrame(raw_data, schema=raw_data_schema)
-- MAGIC     # Write data to Parquet
-- MAGIC     df.write.mode("overwrite").parquet(f"dbfs:/mnt/demo/raw/orders_{file_id}.parquet")
-- MAGIC
-- MAGIC # Generate the initial batch of raw data
-- MAGIC generate_raw_data("dbfs:/mnt/demo/raw", 1)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/mnt/demo/raw/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp, input_file_name
-- MAGIC
-- MAGIC # Start the Bronze stream
-- MAGIC bronze_query = (
-- MAGIC     bronze_stream.writeStream
-- MAGIC     .format("delta")
-- MAGIC     .outputMode("append")  # Append-only mode
-- MAGIC     .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/bronze/checkpoint")
-- MAGIC     .toTable("orders_bronze")
-- MAGIC )
-- MAGIC
-- MAGIC print("Bronze streaming query started.")
-- MAGIC

-- COMMAND ----------

-- Verify Bronze Table
SELECT * FROM orders_bronze;

-- COMMAND ----------

SELECT count(*) FROM orders_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Create Silver Delta Table for Enriched Data

-- COMMAND ----------

-- Drop the table if it exists
DROP TABLE IF EXISTS orders_silver;

-- Create a managed Silver Delta table
CREATE TABLE orders_silver (
    order_id INT,
    customer_id INT,
    customer_name STRING,
    book_id INT,
    book_title STRING,
    quantity INT,
    order_timestamp TIMESTAMP
) USING DELTA;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Static lookup tables for enrichment
-- MAGIC customer_data = [
-- MAGIC     (101, "John Doe"),
-- MAGIC     (102, "Jane Smith"),
-- MAGIC     (103, "Bob Johnson"),
-- MAGIC ]
-- MAGIC book_data = [
-- MAGIC     (1, "Databricks for Beginners"),
-- MAGIC     (2, "Advanced Spark Techniques"),
-- MAGIC     (3, "Delta Lake Mastery"),
-- MAGIC ]
-- MAGIC
-- MAGIC customers_df = spark.createDataFrame(customer_data, ["customer_id", "customer_name"])
-- MAGIC books_df = spark.createDataFrame(book_data, ["book_id", "book_title"])
-- MAGIC
-- MAGIC customers_df.write.mode("overwrite").format("delta").saveAsTable("customers_lookup")
-- MAGIC books_df.write.mode("overwrite").format("delta").saveAsTable("books_lookup")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/mnt/demo/checkpoints/silver/checkpoint", recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import from_unixtime
-- MAGIC
-- MAGIC # Transform Bronze stream to Silver stream
-- MAGIC silver_stream = (
-- MAGIC     spark.readStream
-- MAGIC     .table("orders_bronze")  # Read from Bronze table
-- MAGIC     .join(customers_delta, "customer_id")  # Join with customers
-- MAGIC     .join(books_delta, "book_id")  # Join with books
-- MAGIC     .select(
-- MAGIC         "order_id",
-- MAGIC         "customer_id",
-- MAGIC         "customer_name",
-- MAGIC         "book_id",
-- MAGIC         "book_title",
-- MAGIC         "quantity",
-- MAGIC         from_unixtime("order_timestamp").cast("timestamp").alias("order_timestamp")  # Convert to TIMESTAMP
-- MAGIC     )
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Start the Silver stream
-- MAGIC silver_query = (
-- MAGIC     silver_stream.writeStream
-- MAGIC     .format("delta")
-- MAGIC     .outputMode("append")  # Append mode
-- MAGIC     .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/silver/checkpoint")
-- MAGIC     .toTable("orders_silver")
-- MAGIC )
-- MAGIC
-- MAGIC print("Silver streaming query started.")
-- MAGIC

-- COMMAND ----------

-- Verify Customers Lookup Table
SELECT * FROM customers_lookup;

-- COMMAND ----------

-- Verify Books Lookup Table
SELECT * FROM books_lookup;

-- COMMAND ----------

-- Verify Silver Table
SELECT * FROM orders_silver;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Test the Silver stream transformation
-- MAGIC silver_test = (
-- MAGIC     spark.read.table("orders_bronze")
-- MAGIC     .join(spark.read.table("customers_lookup"), "customer_id")
-- MAGIC     .join(spark.read.table("books_lookup"), "book_id")
-- MAGIC     .select(
-- MAGIC         "order_id",
-- MAGIC         "customer_id",
-- MAGIC         "customer_name",
-- MAGIC         "book_id",
-- MAGIC         "book_title",
-- MAGIC         "quantity",
-- MAGIC         from_unixtime("order_timestamp").cast("timestamp").alias("order_timestamp")
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC # Display the transformation output
-- MAGIC silver_test.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. Create Gold Delta Table for Aggregated Data

-- COMMAND ----------

-- Drop the table if it exists
DROP TABLE IF EXISTS daily_customer_books;

-- Create a managed Gold Delta table
CREATE TABLE daily_customer_books (
    customer_id INT,
    customer_name STRING,
    order_date DATE,
    books_count INT
) USING DELTA;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Delete the checkpoint directory for the Gold stream
-- MAGIC dbutils.fs.rm("dbfs:/mnt/demo/checkpoints/gold/checkpoint", recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import sum, date_trunc, col
-- MAGIC
-- MAGIC # Transform Silver stream to prepare for Gold stream
-- MAGIC gold_stream = (
-- MAGIC     spark.readStream
-- MAGIC     .table("orders_silver")  # Read from the Silver table
-- MAGIC     .groupBy(
-- MAGIC         "customer_id",
-- MAGIC         "customer_name",
-- MAGIC         date_trunc("DAY", col("order_timestamp")).cast("date").alias("order_date")  # Ensure order_date is DATE
-- MAGIC     )
-- MAGIC     .agg(sum("quantity").cast("int").alias("books_count"))  # Aggregate quantity
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import sum, date_trunc
-- MAGIC
-- MAGIC # Start the Gold stream
-- MAGIC gold_query = (
-- MAGIC     gold_stream.writeStream
-- MAGIC     .format("delta")
-- MAGIC     .outputMode("complete")  # Complete mode for aggregations
-- MAGIC     .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/gold/checkpoint")
-- MAGIC     .toTable("daily_customer_books")
-- MAGIC )
-- MAGIC
-- MAGIC print("Gold streaming query started.")

-- COMMAND ----------

-- Verify Gold Table
SELECT * FROM daily_customer_books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 4. Simulate New Data Arrival and Validate the Pipeline

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Simulate new data arrival
-- MAGIC generate_raw_data("dbfs:/mnt/demo/raw", 2)

-- COMMAND ----------

-- Check the updated count in the Gold table
SELECT COUNT(*) FROM daily_customer_books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 5. Stop All Active Streams

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Stop all active streams to release resources
-- MAGIC for stream in spark.streams.active:
-- MAGIC     print(f"Stopping stream: {stream.id}")
-- MAGIC     stream.stop()