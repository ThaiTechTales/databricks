# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Incremental Data Ingestion Using Auto Loader
# MAGIC This notebook demonstrates how to use Auto Loader in Databricks for incremental data ingestion while addressing schema mismatches.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Setting Up the Environment
# MAGIC Define directories and prepare the environment.

# COMMAND ----------

# MAGIC %python
# Import required modules
from pyspark.sql.functions import *
from datetime import date

# Define source and checkpoint directories on DBFS
source_dir = "dbfs:/FileStore/simple-sales-raw"
checkpoint_dir = "dbfs:/FileStore/simple-sales-checkpoint"

# Clear and recreate source directory
dbutils.fs.rm(source_dir, recurse=True)
dbutils.fs.mkdirs(source_dir)

# Clear and recreate checkpoint directory
dbutils.fs.rm(checkpoint_dir, recurse=True)
dbutils.fs.mkdirs(checkpoint_dir)

print("Source and checkpoint directories set up.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Create the Target Delta Table
# MAGIC Define the `simple_sales_updates` table with the expected schema.

# COMMAND ----------

# MAGIC %sql
DROP TABLE IF EXISTS default.simple_sales_updates;

CREATE TABLE default.simple_sales_updates (
    order_id INT,
    order_date DATE,
    amount DOUBLE
) USING DELTA;

-- Verify that the table is created
SHOW TABLES IN default;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Generate Sample Data
# MAGIC Create and load Parquet files into the source directory.

# COMMAND ----------

# MAGIC %python
# Function to generate sample data
def generate_sample_data(directory, file_count=1):
    for i in range(file_count):
        # Create a DataFrame with sample data
        df = spark.createDataFrame(
            [
                (101, date(2025, 1, 1), 99.99),
                (102, date(2025, 1, 2), 49.50),
                (103, date(2025, 1, 3), 29.99),
            ],
            ["id", "order_date", "value"]
        )
        # Write the data to a Parquet file in the source directory
        file_path = f"{directory}/file_{i}.parquet"
        df.write.mode("overwrite").parquet(file_path)
        print(f"File written: {file_path}")

# Generate sample data
generate_sample_data(source_dir, file_count=1)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Verify the Source Data
# MAGIC Confirm that the data exists in the source directory.

# COMMAND ----------

# MAGIC %python
# List files in the directory
print("Files in source directory:")
display(dbutils.fs.ls(source_dir))

# COMMAND ----------

# MAGIC %python
# Check the content of the file
file_path = f"{source_dir}/file_0.parquet"
df = spark.read.format("parquet").load(file_path)
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Configure and Start the Streaming Query
# MAGIC Use Auto Loader to ingest data into the `simple_sales_updates` Delta table.

# COMMAND ----------

# MAGIC %python
# Clear the checkpoint directory to ensure no conflicts
dbutils.fs.rm(checkpoint_dir, recurse=True)
dbutils.fs.mkdirs(checkpoint_dir)

# Define the streaming query
streaming_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")  # Source file format
    .option("cloudFiles.schemaLocation", checkpoint_dir)  # Schema storage
    .load(source_dir)  # Source directory
    .withColumnRenamed("id", "order_id")  # Rename column to match Delta table schema
    .withColumnRenamed("value", "amount")  # Rename column to match Delta table schema
    .withColumn("order_id", col("order_id").cast("int"))  # Ensure order_id is IntegerType
    .withColumn("amount", col("amount").cast("double"))  # Ensure amount is DoubleType
    .select("order_id", "order_date", "amount")  # Select relevant columns
)

# Write the stream to the Delta table
streaming_query = (
    streaming_df.writeStream
    .option("checkpointLocation", checkpoint_dir)  # Checkpoint directory
    .option("mergeSchema", "true")  # Enable schema merging
    .outputMode("append")  # Append mode
    .toTable("default.simple_sales_updates")  # Fully qualified table name
)

print("Streaming query started.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6. Validate the Data Ingestion
# MAGIC Query the `simple_sales_updates` table to verify data ingestion.

# COMMAND ----------

# MAGIC %sql
SELECT * FROM default.simple_sales_updates;

# COMMAND ----------

# MAGIC %sql
SELECT COUNT(*) AS total_records FROM default.simple_sales_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7. Simulate New Data Arrival
# MAGIC Add more data to the source directory to simulate real-time updates.

# COMMAND ----------

# MAGIC %python
# Generate additional data files
generate_sample_data(source_dir, file_count=3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 8. Verify Ingestion of New Data
# MAGIC Confirm that the new data has been processed by the streaming query.

# COMMAND ----------

# MAGIC %sql
SELECT COUNT(*) AS total_records FROM default.simple_sales_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 9. Explore Table History
# MAGIC Delta Lake maintains a history of all operations on the table.

# COMMAND ----------

# MAGIC %sql
DESCRIBE HISTORY default.simple_sales_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 10. Clean Up Resources
# MAGIC Drop the Delta table and delete the directories.

# COMMAND ----------

# MAGIC %sql
DROP TABLE IF EXISTS default.simple_sales_updates;

# COMMAND ----------

# MAGIC %python
# Clean up directories
dbutils.fs.rm(source_dir, recurse=True)
dbutils.fs.rm(checkpoint_dir, recurse=True)

print("Resources cleaned up.")