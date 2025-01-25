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
USE bookstore; -- Set the current database context to bookstore

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
-- MAGIC (spark.readStream
-- MAGIC     .table("books") # 
-- MAGIC     .createOrReplaceTempView("books_streaming_tmp_vw"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Query Streaming Temporary View

-- COMMAND ----------

-- Query the streaming temporary view to display data
-- The query will keep running to listen for new data
-- Stop it manually after inspection
SELECT * FROM books_streaming_tmp_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Apply Transformations

-- COMMAND ----------

-- Apply transformations to count total books per author
SELECT author, COUNT(book_id) AS total_books
FROM books_streaming_tmp_vw
GROUP BY author;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Unsupported Operations Example

-- COMMAND ----------

-- Demonstrating unsupported operation: Sorting streaming data
-- Sorting is not supported directly in streaming queries
-- Use windowing or watermarking for advanced operations
-- SELECT * FROM books_streaming_tmp_vw ORDER BY author;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 6: Persist Streaming Query Results

-- COMMAND ----------

-- Create another streaming temporary view for aggregated data
CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
    SELECT author, COUNT(book_id) AS total_books
    FROM books_streaming_tmp_vw
    GROUP BY author
);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Removing the checkpoint directry
-- MAGIC dbutils.fs.rm("dbfs:/mnt/demo/author_counts_checkpoint", True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Persist the streaming query results into a Delta table
-- MAGIC (spark.table("author_counts_tmp_vw") # Data from the author_counts_tmp_vw streaming temporary view is continuously read as a stream.
-- MAGIC     .writeStream
-- MAGIC     .trigger(processingTime="4 seconds") # When the system processes the next set of data
-- MAGIC     .outputMode("complete") # Configures the output mode of the streaming query. "complete" mode writes the entire result of the aggregation query (e.g., all author counts) every time it processes a new batch of data.
-- MAGIC     .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint" ) # Stores the current state of the streaming job in cloud storage. It’s like a bookmark in a book—if you stop reading, you know where to pick up next.
-- MAGIC     .table("author_counts")) # Delta table stores the aggregated data for querying or analysis.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 7: View Persisted Results

-- COMMAND ----------

-- Query the persisted table to view results
SELECT * FROM author_counts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 8: Add New Data to Source Table

-- COMMAND ----------

-- Add new data to the "books" table
INSERT INTO books VALUES
    ("B06", "Databricks for All", "John Doe", "Data Science", 22.0),
    ("B07", "Mastering Delta Lake", "Jane Smith", "Data Engineering", 45.0),
    ("B08", "Big Data Essentials", "Emily Davis", "Data Engineering", 30.0);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 9: Batch Processing with Streaming

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Use batch processing for a new streaming query
-- MAGIC (spark.table("author_counts_tmp_vw")
-- MAGIC     .writeStream
-- MAGIC     .trigger(availableNow=True)
-- MAGIC     .outputMode("complete")
-- MAGIC     .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
-- MAGIC     .table("author_counts")
-- MAGIC     .awaitTermination())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 10: Verify Updated Results

-- COMMAND ----------

-- Query the updated "author_counts" table to view the final results
SELECT * FROM author_counts;