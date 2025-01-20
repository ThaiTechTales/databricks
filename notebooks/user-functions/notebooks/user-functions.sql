-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC
-- MAGIC ## Exploring Higher-Order Functions and User Defined Functions (UDF) in Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Step 1: Creating and Loading the Dataset
-- MAGIC We will work with a hypothetical e-commerce dataset comprising `customers`, `orders`, and `books` tables. The `books` column in the `orders` table contains hierarchical data.

-- COMMAND ----------

-- Create the `customers` table
CREATE OR REPLACE TEMP VIEW customers AS
SELECT * FROM VALUES
    (1, "alice@example.com"),
    (2, "bob@education.edu"),
    (3, "charlie@nonprofit.org"),
    (4, "david@unknown.xyz")
AS customers(customer_id, email);

-- Create the `orders` table
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
    (101, ARRAY(STRUCT("Book A", 1, 10.0), STRUCT("Book B", 3, 30.0))),
    (102, ARRAY(STRUCT("Book C", 2, 20.0), STRUCT("Book D", 1, 25.0))),
    (103, ARRAY(STRUCT("Book E", 3, 45.0), STRUCT("Book F", 5, 75.0))),
    (104, ARRAY(STRUCT("Book G", 1, 15.0)))
AS orders(order_id, books);

-- Create the `books` table
CREATE OR REPLACE TEMP VIEW books AS
SELECT * FROM VALUES
    ("Book A", "Fiction", 10.0),
    ("Book B", "Non-Fiction", 20.0),
    ("Book C", "Fiction", 15.0),
    ("Book D", "Science", 25.0),
    ("Book E", "History", 30.0),
    ("Book F", "Fiction", 25.0),
    ("Book G", "Non-Fiction", 20.0)
AS books(title, genre, price);

-- COMMAND ----------

-- Verify the data
SELECT * FROM customers;
SELECT * FROM orders;
SELECT * FROM books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Step 2: Higher-Order Functions
-- MAGIC Higher-order functions work directly with complex data types like arrays or maps. Let's explore filtering and transforming arrays.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.1 Filtering Arrays
-- MAGIC The `FILTER` function is used to extract elements from an array that meet a specific condition.

-- COMMAND ----------

-- Filtering books where quantity is greater than or equal to 2
SELECT
    order_id,
    books,
    FILTER(books, b -> b.quantity >= 2) AS multiple_copies
FROM orders;

-- COMMAND ----------

-- Applying a WHERE clause to exclude empty arrays
SELECT order_id, multiple_copies
FROM (
    SELECT
        order_id,
    FILTER(books, b -> b.quantity >= 2) AS multiple_copies
    FROM orders
)
WHERE SIZE(multiple_copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.2 Transforming Arrays
-- MAGIC The `TRANSFORM` function applies a transformation to every element in an array and produces a new array.

-- COMMAND ----------

-- Applying a 20% discount to the `subtotal` of each book in the array
SELECT
    order_id,
    books,
    TRANSFORM(
        books,
        b -> STRUCT(b.title, b.quantity, CAST(b.subtotal * 0.8 AS DECIMAL(10, 2)))
    ) AS discounted_books
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Step 3: User Defined Functions (UDF)
-- MAGIC UDFs allow registering custom logic as reusable functions in SQL queries.

-- COMMAND ----------

-- Creating a UDF to extract domain names from email addresses
CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING
RETURN CONCAT("https://www.", SPLIT(email, "@")[1]);

-- Using the UDF to extract domains
SELECT email, get_url(email) AS domain_url
FROM customers;

-- COMMAND ----------

-- Describing the function
DESCRIBE FUNCTION get_url;

-- Describing the function in detail
DESCRIBE FUNCTION EXTENDED get_url;

-- COMMAND ----------

-- Creating a UDF to categorise domains by type
CREATE OR REPLACE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
    WHEN email LIKE "%.com" THEN "Commercial Business"
    WHEN email LIKE "%.org" THEN "Non-Profit Organisation"
    WHEN email LIKE "%.edu" THEN "Educational Institution"
    ELSE CONCAT("Unknown Domain Type: ", SPLIT(email, "@")[1])
END;

-- Using the UDF to categorise domains
SELECT email, site_type(email) AS domain_category
FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Step 4: Dropping Functions
-- MAGIC Clean up by dropping the user-defined functions.

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;