-- Databricks SQL Notebook for Structured Streaming
-- This file demonstrates Spark Structured Streaming with SQL in Databricks.
-- It includes creating and working with streaming data, transformations, aggregations, and persisting results.

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

-- Create a streaming temporary view on the "books" table
-- MAGIC COMMAND TO ENABLE STREAMING QUERIES
%python
(spark.readStream
    .table("books")
    .createOrReplaceTempView("books_streaming_tmp_vw"))

-- Query the streaming temporary view to display data
-- The query will keep running to listen for new data
-- Stop it manually after inspection
SELECT * FROM books_streaming_tmp_vw;

-- Apply transformations to count total books per author
SELECT author, COUNT(book_id) AS total_books
FROM books_streaming_tmp_vw
GROUP BY author;

-- Demonstrating unsupported operation: Sorting streaming data
-- Sorting is not supported directly in streaming queries
-- Use windowing or watermarking for advanced operations (out of scope here)
-- SELECT * FROM books_streaming_tmp_vw ORDER BY author;

-- Create another streaming temporary view for aggregated data
CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
    SELECT author, COUNT(book_id) AS total_books
    FROM books_streaming_tmp_vw
    GROUP BY author
);

-- Persist the streaming query results into a Delta table
%python
(spark.table("author_counts_tmp_vw")
    .writeStream
    .trigger(processingTime="4 seconds")
    .outputMode("complete")
    .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
    .table("author_counts"))

-- Query the persisted table to view results
SELECT * FROM author_counts;

-- Add new data to the "books" table
INSERT INTO books VALUES
    ("B06", "Databricks for All", "John Doe", "Data Science", 22.0),
    ("B07", "Mastering Delta Lake", "Jane Smith", "Data Engineering", 45.0),
    ("B08", "Big Data Essentials", "Emily Davis", "Data Engineering", 30.0);

-- Use batch processing for a new streaming query
%python
(spark.table("author_counts_tmp_vw")
    .writeStream
    .trigger(availableNow=True)
    .outputMode("complete")
    .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
    .table("author_counts")
    .awaitTermination())

-- Query the updated "author_counts" table to view the final results
SELECT * FROM author_counts;
