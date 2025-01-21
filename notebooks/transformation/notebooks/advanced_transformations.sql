-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Structured Streaming with Databricks SQL
-- MAGIC This notebook demonstrates Spark Structured Streaming with SQL in Databricks.
-- MAGIC It includes creating and working with streaming data, transformations, aggregations, and persisting results.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Setup Database and Tables

-- COMMAND ----------

-- Create a database to store our tables
CREATE DATABASE IF NOT EXISTS bookstore;
USE bookstore;

-- Drop and recreate the "books" table for this demonstration
DROP TABLE IF EXISTS books;
CREATE TABLE books (
       book_id STRING,
       title STRING,
       author STRING,
       genre STRING,
       price DOUBLE
) USING DELTA;

-- Insert initial data into the "books" table
INSERT INTO books VALUES
       ("B01", "Introduction to Databricks", "John Doe", "Data Science", 20.0),
       ("B02", "Advanced Spark Programming", "Jane Smith", "Data Engineering", 35.0),
       ("B03", "Delta Lake for Beginners", "Emily Davis", "Data Engineering", 25.0),
       ("B04", "Machine Learning Basics", "Michael Brown", "Data Science", 30.0),
       ("B05", "Streaming Data Analysis", "Chris White", "Data Engineering", 40.0);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Create a Streaming Temporary View

-- COMMAND ----------

-- MAGIC %python
(spark.readStream
       .table("books")
       .createOrReplaceTempView("books_streaming_tmp_vw"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Query Streaming Temporary View

-- COMMAND ----------

-- Query the streaming temporary view to display data
-- The query will keep running to listen for new data
-- Stop it manually after inspection
-- MAGIC %sql
SELECT * FROM books_streaming_tmp_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Apply Transformations

-- COMMAND ----------

-- Apply transformations to count total books per author
-- MAGIC %sql
SELECT author, COUNT(book_id) AS total_books
FROM books_streaming_tmp_vw
GROUP BY author;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Unsupported Operations Example

-- COMMAND ----------

-- Demonstrating unsupported operation: Sorting streaming data
-- Sorting is not supported directly in streaming queries
-- Use windowing or watermarking for advanced operations (out of scope here)
-- MAGIC %sql
-- SELECT * FROM books_streaming_tmp_vw ORDER BY author;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 6: Persist Streaming Query Results

-- COMMAND ----------

-- Create another streaming temporary view for aggregated data
-- MAGIC %sql
CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
       SELECT author, COUNT(book_id) AS total_books
       FROM books_streaming_tmp_vw
       GROUP BY author
);

-- COMMAND ----------

-- Persist the streaming query results into a Delta table
-- MAGIC %python
(spark.table("author_counts_tmp_vw")
       .writeStream
       .trigger(processingTime="4 seconds")
       .outputMode("complete")
       .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
       .table("author_counts"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 7: View Persisted Results

-- COMMAND ----------

-- Query the persisted table to view results
-- MAGIC %sql
SELECT * FROM author_counts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 8: Add New Data to Source Table

-- COMMAND ----------

-- Add new data to the "books" table
-- MAGIC %sql
INSERT INTO books VALUES
       ("B06", "Databricks for All", "John Doe", "Data Science", 22.0),
       ("B07", "Mastering Delta Lake", "Jane Smith", "Data Engineering", 45.0),
       ("B08", "Big Data Essentials", "Emily Davis", "Data Engineering", 30.0);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 9: Batch Processing with Streaming

-- COMMAND ----------

-- Use batch processing for a new streaming query
-- MAGIC %python
(spark.table("author_counts_tmp_vw")
       .writeStream
       .trigger(availableNow=True)
       .outputMode("complete")
       .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
       .table("author_counts")
       .awaitTermination())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 10: Verify Updated Results

-- COMMAND ----------

-- Query the updated "author_counts" table to view the final results
-- MAGIC %sql
SELECT * FROM author_counts;