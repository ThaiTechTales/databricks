# Databricks notebook source
# MAGIC %md
# MAGIC ## Incremental Data Ingestion Using Auto Loader
# MAGIC This notebook demonstrates incremental data ingestion using Databricks Auto Loader.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Setting Up the Environment

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import *
from datetime import datetime

# Define source and checkpoint directories
source_dir = "dbfs:/mnt/demo/simple-sales-raw"
checkpoint_dir = "dbfs:/mnt/demo/simple-sales-checkpoint"

# Recreate source directory (if needed)
dbutils.fs.mkdirs(source_dir)

# Helper function to simulate data generation
def generate_simple_data(directory, file_count=1):
    for i in range(file_count):
        # Create a simple DataFrame with sample data
        df = spark.createDataFrame(
            [
                (1, "2025-01-01", 100.50),
                (2, "2025-01-02", 200.75),
                (3, "2025-01-03", 150.25),
            ],
            ["id", "order_date", "value"]
        ).withColumn("order_date", col("order_date").cast("date"))
        # Write the DataFrame to Parquet
        df.write.mode("overwrite").parquet(f"{directory}/file_{i}.parquet")

# Generate initial dataset
generate_simple_data(source_dir, file_count=1)

# List the files in the source directory to confirm setup
files = dbutils.fs.ls(source_dir)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create the Target Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.simple_sales_updates (
# MAGIC     order_id INT,
# MAGIC     order_date DATE,
# MAGIC     amount DOUBLE
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Configure Auto Loader Streaming Query

# COMMAND ----------

# Define the Auto Loader streaming query
streaming_df = (
    spark.readStream
    .format("cloudFiles")  # Use Auto Loader
    .option("cloudFiles.format", "parquet")  # Source file format
    .option("cloudFiles.schemaLocation", checkpoint_dir)  # Schema storage for incremental updates
    .load(source_dir)  # Source directory
    .withColumnRenamed("id", "order_id")  # Align schema with Delta table
    .withColumnRenamed("value", "amount")  # Align schema
    .select("order_id", "order_date", "amount")  # Select relevant columns
)

# Write the streaming data to the Delta table
(streaming_df.writeStream
    .option("checkpointLocation", checkpoint_dir)  # Checkpoint directory
    .option("mergeSchema", "true")  # Enable schema merging
    .outputMode("append")  # Append new data
    .toTable("default.simple_sales_updates")  # Specify target Delta table
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Query the Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the data in the Delta table
# MAGIC SELECT * FROM default.simple_sales_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count the total number of records
# MAGIC SELECT COUNT(*) AS total_records FROM default.simple_sales_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Simulate New Data Arrival

# COMMAND ----------

# Generate additional data files to simulate real-time data arrival
generate_simple_data(source_dir, file_count=2)

# List the new files in the source directory
files = dbutils.fs.ls(source_dir)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Validate Data Ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify that the new data has been ingested into the Delta table
# MAGIC SELECT COUNT(*) AS total_records FROM default.simple_sales_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Explore Delta Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the history of the Delta table
# MAGIC DESCRIBE HISTORY default.simple_sales_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Clean Up Resources

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the Delta table
# MAGIC DROP TABLE IF EXISTS default.simple_sales_updates;

# COMMAND ----------

# Remove the source and checkpoint directories
dbutils.fs.rm(source_dir, recurse=True)
dbutils.fs.rm(checkpoint_dir, recurse=True)