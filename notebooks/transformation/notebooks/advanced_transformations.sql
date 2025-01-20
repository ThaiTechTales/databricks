-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Advanced Transformations with Spark SQL
-- MAGIC This notebook demonstrates advanced transformations in Spark SQL using JSON parsing, array handling, joins, pivots, and more.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Load Datasets into Tables
-- MAGIC Load the JSON datasets into Delta tables. Ensure the files are available at the specified path.

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/transformation/data/

-- COMMAND ----------

-- Drop existing tables to avoid conflicts
DROP TABLE IF EXISTS customers; -- Contains string value of the json objects
DROP TABLE IF EXISTS customers_json;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS books;

-- Create tables from JSON files
CREATE TABLE IF NOT EXISTS customers AS
SELECT *
FROM
       json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/transformation/data/customers-string.json`;

CREATE TABLE IF NOT EXISTS customers_json AS
SELECT *
FROM
       json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/transformation/data/customers-nested-json.json`;    

CREATE TABLE IF NOT EXISTS orders AS
SELECT
    *
FROM
       json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/transformation/data/orders.json`;
CREATE TABLE IF NOT EXISTS books AS
SELECT
    *
FROM
       json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/transformation/data/books.json`;

-- COMMAND ----------

select * FROM customers;

-- COMMAND ----------

select * FROM customers_json;

-- COMMAND ----------

select * FROM orders;

-- COMMAND ----------

select * FROM books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2a: Parsing JSON Data w/ nested Struct
-- MAGIC Parse nested JSON fields from the `customers` table.
-- MAGIC
-- MAGIC Note that this has nested JSON objects, which is a `STRUCT` and thus `.` is typically used to extract values.

-- COMMAND ----------

-- Query to access nested STRUCT fields
SELECT customer_id, 
       profile.first_name AS first_name, 
       profile.address.country AS country 
FROM customers_json;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2b: Parsing JSON Data w/ String Values
-- MAGIC Parse nested JSON fields from the `customers-string` table.
-- MAGIC
-- MAGIC Note this has the `profile` field's value as a string containing a JSON object

-- COMMAND ----------

-- Query to access nested STRUCT fields
SELECT customer_id, 
       profile:first_name as first_name,
       profile:address.country as country
FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Convert JSON String to Struct Type
-- MAGIC Use the `from_json` function to convert the JSON string into a structured type.

-- COMMAND ----------

-- Derive schema for JSON and convert to struct type
CREATE OR REPLACE TEMP VIEW parsed_customers AS
SELECT customer_id, 
       from_json(profile, schema_of_json('{"first_name":"Alice","last_name":"Smith","gender":"Female","address":{"street":"123 Elm Street","city":"Springfield","country":"USA"}}')) AS profile_struct
FROM customers;

-- COMMAND ----------

SELECT profile 
FROM customers 
LIMIT 1 -- Only one row

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Flatten Struct Fields into Columns
-- MAGIC Use the dot syntax to flatten the `struct` fields into individual columns.

-- COMMAND ----------

-- Flatten struct fields
CREATE OR REPLACE TEMP VIEW customers_final AS
SELECT customer_id, 
       profile_struct.* -- selects the customer_id column and all fields within the profile_struct column.
FROM parsed_customers;

-- COMMAND ----------

DESCRIBE customers_final;

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Explode Arrays in the `orders` Table
-- MAGIC Use the `explode` function to create a row for each element of an array.

-- COMMAND ----------

--- Without explode, each row will show the entire array of books for the respective order_id and customer_id
SELECT order_id, 
       customer_id, 
       books
FROM orders;

-- COMMAND ----------

-- Explode the books array in orders, 
-- Each element of the books array is returned as a separate row, 
SELECT order_id, 
       customer_id, 
       explode(books) AS book
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 6: Collect Sets and Flatten Arrays
-- MAGIC Use `collect_set` to aggregate values and `flatten` to simplify nested arrays.
-- MAGIC
-- MAGIC - It will collect unique values from a column into a set. Note, sets are a data structure that stores a collection of unique values.
-- MAGIC - Output the values as an array

-- COMMAND ----------

-- Without Collect Sets and Flatten
SELECT customer_id,
       order_id AS order_set,
       books.book_id AS books_set
FROM orders

-- COMMAND ----------

-- Collect sets of order IDs and book IDs
SELECT customer_id,
       collect_set(order_id) AS order_set,
       collect_set(books.book_id) AS books_set
FROM orders

-- The above query returns a single row for each customer but with GROUP BY, all rows for the same customer are placed in a single group. 
GROUP BY customer_id;

-- COMMAND ----------

-- Flatten and deduplicate arrays
-- array_distinct: Removes duplicates from the flattened array
SELECT customer_id,
       array_distinct(flatten(collect_set(books.book_id))) AS unique_books
FROM orders
GROUP BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 7: Join Operations
-- MAGIC Joins in SQL are used to combine rows from two or more tables based on a related column. They allow you to retrieve data spread across multiple tables by matching rows based on a specified condition.
-- MAGIC
-- MAGIC Join `orders` with `books` to enrich data by adding book details.

-- COMMAND ----------

-- Create a view for enriched orders
-- SELECT *: Includes all columns from the orders table.

-- explode(books):
-- The books column in the orders table contains an array.
-- explode(books) creates a new row for each element in the array, effectively "flattening" the array.
-- The resulting rows will have a new column book, containing one element of the array per row.

-- Alias o:
-- The subquery is aliased as o, so you can reference its columns with a prefix (e.g., o.order_id, o.book.book_id).

CREATE OR REPLACE VIEW orders_enriched AS
SELECT o.order_id, 
       o.customer_id, 
       b.title, 
       b.author, 
       b.category
FROM (SELECT *, explode(books) AS book FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

-- COMMAND ----------

-- Understanding Select Query
SELECT *
FROM orders

-- COMMAND ----------

-- Understanding Select Query
SELECT *, explode(books) AS book
FROM orders;

-- COMMAND ----------

SELECT * 
FROM orders_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 8: Set Operations
-- MAGIC Perform `UNION`, `INTERSECT`, and `MINUS` operations on the orders data.

-- COMMAND ----------

-- Union operation with matching columns
-- Combines rows from both queries.	
SELECT order_id, customer_id FROM orders
UNION 
SELECT order_id, customer_id FROM orders_enriched;

-- COMMAND ----------

-- Intersect operation with matching columns
-- Finds rows common to both queries.	
SELECT order_id, customer_id FROM orders
INTERSECT
SELECT order_id, customer_id FROM orders_enriched;

-- COMMAND ----------

-- Minus operation with matching columns
-- Finds rows in the first query but not in the second.	
SELECT order_id, customer_id
FROM orders
EXCEPT
SELECT order_id, customer_id
FROM orders_enriched;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 9: Reshaping Data with Pivot
-- MAGIC A pivot is a technique in SQL used to transform rows of data into columns. It’s commonly used to reorganise and summarise data, making it easier to analyse relationships or trends.
-- MAGIC
-- MAGIC Use the `PIVOT` clause to reshape data for aggregation and dashboarding.

-- COMMAND ----------

-- Create a pivot table for book quantities per customer
CREATE OR REPLACE TABLE transactions AS
SELECT * 
FROM (
       SELECT oe.customer_id, 
       b.book_id, 
       oe.order_id
       FROM orders_enriched oe
       JOIN books b ON oe.title = b.title
) PIVOT (
       count(order_id) FOR book_id IN ('B001', 'B002', 'B003')
);

-- COMMAND ----------

-- View the transactions table
SELECT * FROM transactions;


-- Testing the Pivot operation with basic table

-- Create the sales table
CREATE OR REPLACE TABLE sales (
       customer VARCHAR(50),
       product VARCHAR(50),
       month VARCHAR(10),
       amount INT
);

-- Insert data into the sales table
INSERT INTO sales (customer, product, month, amount) VALUES
('Alice', 'Laptop', 'Jan', 1200),
('Alice', 'Phone', 'Jan', 800),
('Bob', 'Laptop', 'Feb', 1500),
('Bob', 'Tablet', 'Feb', 700),
('Alice', 'Laptop', 'Mar', 1300),
('Bob', 'Phone', 'Mar', 900);


-- COMMAND ----------

-- Testing the Pivot operation with basic table

SELECT *
FROM sales;

-- COMMAND ----------

-- Testing the Pivot operation with basic table

-- Pivot the data so that:
-- Each month becomes a column.
-- The values in these columns represent the sum of amount for each customer and product.

-- Columns to Pivot:
-- The PIVOT clause specifies that month values ('Jan', 'Feb', 'Mar') will become columns.
-- Aggregation:
-- The SUM(amount) function calculates the total amount for each combination of customer, product, and month.
-- Nulls for Missing Data:
-- If there’s no data for a specific combination (e.g., Alice bought nothing in Feb), the result is NULL.

SELECT *
FROM (
       SELECT customer, product, month, amount
       FROM sales
) PIVOT (
       SUM(amount) FOR month IN ('Jan', 'Feb', 'Mar')
);