-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Incremental Data Ingestion Using Auto Loader
-- MAGIC This notebook demonstrates how to use Auto Loader in Databricks for incremental data ingestion.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Setting Up the Environment
-- MAGIC Before starting, ensure the source directory and required tables are created.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC # Directory setup
-- MAGIC source_dir = "dbfs:/mnt/demo/sales-raw"
-- MAGIC checkpoint_dir = "dbfs:/mnt/demo/sales_checkpoint"
-- MAGIC
-- MAGIC # Helper function to simulate new data ingestion
-- MAGIC def generate_sample_data(directory, file_count=1):
-- MAGIC     for i in range(file_count):
-- MAGIC         df = spark.range(1000).withColumn("order_id", expr("id"))
-- MAGIC         df = df.withColumn("order_date", lit(datetime.now()))
-- MAGIC         df = df.withColumn("customer_id", expr("id % 100"))
-- MAGIC         df = df.withColumn("amount", expr("rand() * 100"))
-- MAGIC         df.write.mode("overwrite").parquet(f"{directory}/file_{i}.parquet")
-- MAGIC
-- MAGIC # Create initial dataset
-- MAGIC dbutils.fs.mkdirs(source_dir)
-- MAGIC generate_sample_data(source_dir)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Explore the Source Directory
-- MAGIC Listing all the files in the source directory to confirm the setup.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(source_dir)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. Auto Loader Stream Setup
-- MAGIC The following code demonstrates the use of Auto Loader to read and process data incrementally.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC     .format("cloudFiles")  # Use Auto Loader format
-- MAGIC     .option("cloudFiles.format", "parquet")  # Source file format
-- MAGIC     .option("cloudFiles.schemaLocation", checkpoint_dir)  # Schema storage for incremental updates
-- MAGIC     .load(source_dir)#-- Source directory
-- MAGIC     .writeStream
-- MAGIC     .option("checkpointLocation", checkpoint_dir)  # Checkpoint directory
-- MAGIC     .outputMode("append")  # Append mode for incremental processing
-- MAGIC     .table("sales_updates")  # Target table
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 4. Query the Target Table
-- MAGIC Verify the data ingestion process by querying the target table.

-- COMMAND ----------

SELECT * FROM sales_updates LIMIT 10;

-- COMMAND ----------

SELECT count(*) AS total_records FROM sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 5. Simulating New Data Arrival
-- MAGIC Add new data files to the source directory to simulate real-time ingestion.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC generate_sample_data(source_dir, file_count=2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(source_dir)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 6. Validate Ingestion of New Data
-- MAGIC Confirm the new data is ingested by the Auto Loader stream.

-- COMMAND ----------

SELECT count(*) AS total_records FROM sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 7. Table History
-- MAGIC Explore the table history to understand how Delta Lake manages versions.

-- COMMAND ----------

DESCRIBE HISTORY sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 8. Cleaning Up Resources
-- MAGIC Remove the created resources to maintain a clean environment.

-- COMMAND ----------

DROP TABLE IF EXISTS sales_updates;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm(source_dir, True)
-- MAGIC dbutils.fs.rm(checkpoint_dir, True)
