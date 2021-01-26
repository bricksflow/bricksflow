# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #1: Create new table from CSV

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Requirements for running this notebook

# COMMAND ----------

# DBTITLE 0,1.
# MAGIC %sql
# MAGIC /* Setting up databases */
# MAGIC create database if not exists dev_bronze_covid;
# MAGIC create database if not exists dev_silver_covid;
# MAGIC create database if not exists dev_gold_reporting

# COMMAND ----------

# MAGIC %md ## Running the first Bricksflow-powered notebook

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Bricksflow framework and all project dependencies

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as f
from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver, tableParams
from datalakebundle.table.TableManager import TableManager

# COMMAND ----------

@dataFrameLoader(tableParams("bronze_covid.tbl_template_1_mask_usage").source_csv_path)
def read_csv_mask_usage(source_csv_path: str, spark: SparkSession, logger: Logger):
    logger.info(f"Reading CSV from source path: `{source_csv_path}`.")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")  # Tip: it might be better idea to define schema!
        .load(source_csv_path)
        .limit(10)  # only for test
    )

# COMMAND ----------

# MAGIC %md #### Using display=True to display transformation outputs

# COMMAND ----------

@transformation(read_csv_mask_usage, display=True)
def add_column_insert_ts(df: DataFrame, logger: Logger):
    logger.info("Adding Insert timestamp")
    return df.withColumn("INSERT_TS", f.lit(datetime.now()))

# COMMAND ----------

# MAGIC %md #### Saving results to fresh table

# COMMAND ----------

@dataFrameSaver(add_column_insert_ts)
def save_table_bronze_covid_tbl_template_1_mask_usage(df: DataFrame, logger: Logger, table_manager: TableManager):
    # Recreate = remove table and create again
    table_manager.recreate("bronze_covid.tbl_template_1_mask_usage")

    output_table_name = table_manager.getName("bronze_covid.tbl_template_1_mask_usage")
    
    logger.info(f"Saving data to table: {output_table_name}")
    
    (
        df.select("COUNTYFP", "NEVER", "RARELY", "SOMETIMES", "FREQUENTLY", "ALWAYS", "INSERT_TS")
        .write.option("partitionOverwriteMode", "dynamic")
        .insertInto(output_table_name)
    )

    logger.info(f"Data successfully saved to: {output_table_name}")
