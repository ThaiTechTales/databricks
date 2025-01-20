-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Exploring Higher-Order Functions and User Defined Functions (UDF) in Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Step 1: Creating and Loading the Dataset
-- MAGIC The `books` column in the `orders` table contains hierarchical data.
-- MAGIC
-- MAGIC Hierarchical data refers to information organised in a tree-like or nested structure where each "parent" can have one or more "child" elements.
-- MAGIC
-- MAGIC In the context of the orders table, the books column contains an array of structs, making it hierarchical because:
-- MAGIC
-- MAGIC Array: The books column is an array, which is a collection of elements (in this case, structs).
-- MAGIC
-- MAGIC Struct: Each element in the array is a struct, representing a book, with fields such as title, quantity, and subtotal.
-- MAGIC
-- MAGIC In simpler terms, the data is not flat (like a typical table row with primitive data types) but has levels of nesting. Hierarchical data allows storing complex relationships within a single column

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
    (101, ARRAY(STRUCT("Book A" AS title, 1 AS quantity, 10.0 AS subtotal), 
                STRUCT("Book B" AS title, 3 AS quantity, 30.0 AS subtotal))),
    (102, ARRAY(STRUCT("Book C" AS title, 2 AS quantity, 20.0 AS subtotal), 
                STRUCT("Book D" AS title, 1 AS quantity, 25.0 AS subtotal))),
    (103, ARRAY(STRUCT("Book E" AS title, 3 AS quantity, 45.0 AS subtotal), 
                STRUCT("Book F" AS title, 5 AS quantity, 75.0 AS subtotal))),
    (104, ARRAY(STRUCT("Book G" AS title, 1 AS quantity, 15.0 AS subtotal)))
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

-- COMMAND ----------

SELECT * FROM orders;

-- COMMAND ----------

SELECT * FROM books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Step 2: Higher-Order Functions
-- MAGIC Higher-order functions work directly with complex data types like arrays or maps.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.1 Filtering Arrays
-- MAGIC The `FILTER` function is used to extract elements from an array that meet a specific condition.

-- COMMAND ----------

-- Filter books where the quantity is greater than or equal to 2.
-- A lambda function is an anonymous function used to apply logic to elements in a collection.
-- In this query:
-- `b` represents each element in the `books` array.
-- `b.quantity >= 2` is the condition used to filter elements.
-- Syntax: FILTER(array, element -> condition)
-- `array`: The input array to process.
-- `element`: A placeholder for each element in the array.
-- `condition`: The logic applied to decide whether an element is included.
-- The `->` operator maps the element to the logic, indicating: "Take this element and apply this condition."

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
-- Size(): Returns the number of elements in the array
WHERE SIZE(multiple_copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.2 Transforming Arrays
-- MAGIC The `TRANSFORM` function applies a transformation to every element in an array and produces a new array.

-- COMMAND ----------

-- Testing TRANSFORM with a basic example.

-- Create a temporary view called `employees`.
-- - This view contains employee details, including:
--   - `employee_id`: Unique identifier for each employee.
--   - `name`: The employee's name.
--   - `monthly_salaries`: An array of the employee's monthly salaries.
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
    (1, "Alice", ARRAY(3000, 3200, 3500)),
    (2, "Bob", ARRAY(2500, 2700, 2800)),
    (3, "Charlie", ARRAY(4000, 4100, 4200))
AS employees(employee_id, name, monthly_salaries);

-- COMMAND ----------

-- Check the original data
SELECT * FROM employees;

-- COMMAND ----------

-- Applying a 10% bonus to each monthly salary
-- TRANSFORM: Modifies each element of the books array.
SELECT
    employee_id,
    name,
    monthly_salaries,
    TRANSFORM(
        monthly_salaries,
        salary -> salary * 1.1
    ) AS salaries_with_bonus
FROM employees;

-- COMMAND ----------

-- Applying a 20% discount to the `subtotal` of each book in the array
SELECT order_id, books,
    TRANSFORM (
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

-- COMMAND ----------

-- Using the UDF to extract domains
SELECT email, get_url(email) AS domain_url
FROM customers;

-- COMMAND ----------

-- Describing the function
DESCRIBE FUNCTION get_url;

-- COMMAND ----------

-- Describing the function in detail
DESCRIBE FUNCTION EXTENDED get_url;

-- COMMAND ----------

-- Creating a UDF to categorise domains by type.

-- LIKE Operator:
-- - Used to match a string against a pattern.
-- - Patterns can include wildcards for flexible matches:
--   - `%`: Matches zero or more characters.

-- Syntax:
-- column_name LIKE 'pattern'

-- THEN Clause:
-- - Part of a CASE statement, specifies the value to return if the associated condition evaluates to TRUE.
-- - Components of a CASE statement:
--   - WHEN condition: Specifies a condition to evaluate.
--   - THEN result: The value returned if the condition is TRUE.
--   - ELSE default_result (Optional): Specifies the value returned if none of the WHEN conditions are TRUE.
--   - END: Marks the end of the CASE statement.

-- CASE Statement:
-- - Evaluates conditions sequentially.
-- - Stops at the first TRUE condition and returns the corresponding THEN value.
-- - If no conditions are TRUE, it defaults to the ELSE value (if provided).

CREATE OR REPLACE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
    WHEN email LIKE "%.com" THEN "Commercial Business"
    WHEN email LIKE "%.org" THEN "Non-Profit Organisation"
    WHEN email LIKE "%.edu" THEN "Educational Institution"
    ELSE CONCAT("Unknown Domain Type: ", SPLIT(email, "@")[1])
END;

-- COMMAND ----------

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