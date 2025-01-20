-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Learning Spark SQL: Advanced Transformations and User-Defined Functions (UDFs)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Step 1: Create Example Dataset
-- MAGIC Let's create example datasets for customers, orders, and books. These datasets simulate a real-world scenario of a bookstore.

-- COMMAND ----------

-- Create customers table
CREATE OR REPLACE TEMP VIEW customers AS
SELECT * FROM VALUES
    (1, 'john.doe@example.com', '{"first_name":"John","last_name":"Doe","address":{"street":"123 Elm St","city":"Springfield","country":"USA"}}'),
    (2, 'jane.smith@example.org', '{"first_name":"Jane","last_name":"Smith","address":{"street":"456 Oak St","city":"Springfield","country":"USA"}}'),
    (3, 'alice.wonder@example.edu', '{"first_name":"Alice","last_name":"Wonder","address":{"street":"789 Pine St","city":"Denver","country":"USA"}}')
AS customers(customer_id, email, profile);

-- Create orders table
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
    (101, 1, '[{"book_id":"B01","quantity":2,"subtotal":30.0},{"book_id":"B02","quantity":1,"subtotal":15.0}]'),
    (102, 2, '[{"book_id":"B03","quantity":1,"subtotal":20.0}]'),
    (103, 3, '[{"book_id":"B01","quantity":3,"subtotal":45.0},{"book_id":"B04","quantity":1,"subtotal":25.0}]')
AS orders(order_id, customer_id, books);

-- Create books table
CREATE OR REPLACE TEMP VIEW books AS
SELECT * FROM VALUES
    ('B01', 'Spark SQL Mastery', 'John Smith', 'Technology'),
    ('B02', 'Python for Data Science', 'Jane Doe', 'Programming'),
    ('B03', 'Machine Learning 101', 'Alice Wonder', 'AI & ML'),
    ('B04', 'Database Design Principles', 'Mike Ross', 'Technology')
AS books(book_id, title, author, category);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Step 2: Describe the Customers Table
-- MAGIC View the schema and structure of the customers table.

-- COMMAND ----------

DESCRIBE EXTENDED customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Step 3: Extract Nested JSON Data
-- MAGIC Use JSON parsing functions to extract and flatten the nested structure of the `profile` column.

-- COMMAND ----------

-- Parse the JSON string in the profile column
CREATE OR REPLACE TEMP VIEW customers_parsed AS
SELECT
    customer_id,
    email,
    from_json(profile, schema_of_json('{"first_name":"John","last_name":"Doe","address":{"street":"123 Elm St","city":"Springfield","country":"USA"}}')) AS parsed_profile
FROM customers;

-- Flatten the JSON data into separate columns
SELECT
    customer_id,
    email,
    parsed_profile.first_name AS first_name,
    parsed_profile.last_name AS last_name,
    parsed_profile.address.street AS street,
    parsed_profile.address.city AS city,
    parsed_profile.address.country AS country
FROM customers_parsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Step 4: Array Transformations in the Orders Table
-- MAGIC Learn to filter and transform arrays within the `books` column.

-- COMMAND ----------

-- Filter books where quantity >= 2
SELECT
    order_id,
    books,
    FILTER(from_json(books, 'ARRAY<STRUCT<book_id: STRING, quantity: INT, subtotal: FLOAT>>'), b -> b.quantity >= 2) AS filtered_books
FROM orders;

-- Apply a discount on subtotals in the books array
SELECT
    order_id,
    books,
    TRANSFORM(
        from_json(books, 'ARRAY<STRUCT<book_id: STRING, quantity: INT, subtotal: FLOAT>>'),
        b -> named_struct('book_id', b.book_id, 'quantity', b.quantity, 'subtotal', b.subtotal * 0.9)
    ) AS discounted_books
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Step 5: User-Defined Functions (UDFs)
-- MAGIC Create and use UDFs to add custom logic.

-- COMMAND ----------

-- Create a UDF to extract domain from email
CREATE OR REPLACE FUNCTION extract_domain(email STRING)
RETURNS STRING
RETURN split(email, '@')[1];

-- Use the UDF to extract domain
SELECT email, extract_domain(email) AS domain
FROM customers;

-- COMMAND ----------

-- Create a UDF to categorise email domains
CREATE OR REPLACE FUNCTION domain_category(email STRING)
RETURNS STRING
RETURN CASE
    WHEN email LIKE "%.com" THEN "Commercial"
    WHEN email LIKE "%.org" THEN "Organisation"
    WHEN email LIKE "%.edu" THEN "Education"
    ELSE "Other"
END;

-- Use the UDF for categorisation
SELECT email, domain_category(email) AS category
FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Step 6: Explode Arrays and Join Operations
-- MAGIC Learn to use `explode` and join tables for enriched datasets.

-- COMMAND ----------

-- Explode the books array
CREATE OR REPLACE TEMP VIEW exploded_books AS
SELECT
    order_id,
    customer_id,
    EXPLODE(from_json(books, 'ARRAY<STRUCT<book_id: STRING, quantity: INT, subtotal: FLOAT>>')) AS book
FROM orders;

-- Join with books table
SELECT
    e.order_id,
    e.customer_id,
    e.book.book_id,
    e.book.quantity,
    e.book.subtotal,
    b.title,
    b.author,
    b.category
FROM exploded_books e
INNER JOIN books b
ON e.book.book_id = b.book_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Step 7: Set Operations and Pivot Tables
-- MAGIC Use set operations like `UNION` and create pivot tables for analysis.

-- COMMAND ----------

-- Union operation on orders table (simulate old and new data)
SELECT * FROM orders
UNION
SELECT * FROM VALUES
    (104, 1, '[{"book_id":"B05","quantity":1,"subtotal":12.0}]')
AS orders(order_id, customer_id, books);

-- Pivot table for aggregated analysis
SELECT *
FROM (
    SELECT customer_id, order_id, book.book_id, book.quantity
    FROM exploded_books
) PIVOT (
    SUM(quantity) FOR book_id IN ('B01', 'B02', 'B03', 'B04', 'B05')
);
