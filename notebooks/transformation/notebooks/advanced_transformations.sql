-- Databricks Notebook Source
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
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS books;

-- Create tables from JSON files
CREATE TABLE customers USING json OPTIONS (path 'file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/transformation/data/customers.json');
CREATE TABLE orders USING json OPTIONS (path 'file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/transformation/data/orders.json');
CREATE TABLE books USING json OPTIONS (path 'file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/transformation/data/books.json');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Parsing JSON Data
-- MAGIC Parse nested JSON fields from the `customers` table.

-- COMMAND ----------

-- Query to access nested JSON fields
SELECT customer_id, 
    profile:first_name AS first_name, 
    profile:address:country AS country 
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

-- MAGIC %md
-- MAGIC ## Step 4: Flatten Struct Fields into Columns
-- MAGIC Use the dot syntax to flatten the `struct` fields into individual columns.

-- COMMAND ----------

-- Flatten struct fields
CREATE OR REPLACE TEMP VIEW customers_final AS
SELECT customer_id, 
    profile_struct.*
FROM parsed_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Explode Arrays in the `orders` Table
-- MAGIC Use the `explode` function to create a row for each element of an array.

-- COMMAND ----------

-- Explode the books array in orders
SELECT order_id, 
    customer_id, 
    explode(books) AS book
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 6: Collect Sets and Flatten Arrays
-- MAGIC Use `collect_set` to aggregate values and `flatten` to simplify nested arrays.

-- COMMAND ----------

-- Collect sets of order IDs and book IDs
SELECT customer_id,
    collect_set(order_id) AS order_set,
    collect_set(book.book_id) AS books_set
FROM orders
GROUP BY customer_id;

-- COMMAND ----------

-- Flatten and deduplicate arrays
SELECT customer_id,
    array_distinct(flatten(collect_set(book.book_id))) AS unique_books
FROM orders
GROUP BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 7: Join Operations
-- MAGIC Join `orders` with `books` to enrich data by adding book details.

-- COMMAND ----------

-- Create a view for enriched orders
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

-- MAGIC %md
-- MAGIC ## Step 8: Set Operations
-- MAGIC Perform `UNION`, `INTERSECT`, and `MINUS` operations on the orders data.

-- COMMAND ----------

-- Union operation
SELECT * FROM orders
UNION 
SELECT * FROM orders_enriched;

-- COMMAND ----------

-- Intersect operation
SELECT * FROM orders
INTERSECT
SELECT * FROM orders_enriched;

-- COMMAND ----------

-- Minus operation
SELECT * FROM orders
MINUS
SELECT * FROM orders_enriched;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 9: Reshaping Data with Pivot
-- MAGIC Use the `PIVOT` clause to reshape data for aggregation and dashboarding.

-- COMMAND ----------

-- Create a pivot table for book quantities per customer
CREATE OR REPLACE TABLE transactions AS
SELECT * 
FROM (
    SELECT customer_id, 
            book.book_id AS book_id, 
            book.quantity AS quantity
    FROM orders_enriched
) PIVOT (
    sum(quantity) FOR book_id IN ('B001', 'B002', 'B003')
);

-- COMMAND ----------

-- View the transactions table
SELECT * FROM transactions;
