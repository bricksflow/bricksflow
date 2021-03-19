# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #1: Create new table from CSV

# COMMAND ----------

# MAGIC %md ## Running the first Bricksflow-powered notebook

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Bricksflow framework and all project dependencies

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

import os
from datetime import datetime
from pyspark.sql import functions as f
from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import data_frame_loader, transformation, data_frame_saver, table_params
from datalakebundle.table.TableManager import TableManager

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating empty databases

# COMMAND ----------

spark.sql(f"create database if not exists {os.environ['APP_ENV']}_bronze_covid;")  # noqa: F821
spark.sql(f"create database if not exists {os.environ['APP_ENV']}_silver_covid;")  # noqa: F821
spark.sql(f"create database if not exists {os.environ['APP_ENV']}_gold_reporting;")  # noqa: F821

# COMMAND ----------

# MAGIC %md #### Reading a CSV file
# MAGIC
# MAGIC Let's use `@data_frame_loader` notebook function to load our first data! Notice how the `logger` object is used to print logging information to the output.

# COMMAND ----------


@data_frame_loader(table_params("bronze_covid.tbl_template_1_mask_usage").source_csv_path)
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

# MAGIC %md
# MAGIC
# MAGIC #### Transform data by adding new column
# MAGIC
# MAGIC Using the `@transformation` notebook function. Notice the usage of `display=True` to display transformation outputs

# COMMAND ----------


@transformation(read_csv_mask_usage, display=True)
def add_column_insert_ts(df: DataFrame, logger: Logger):
    logger.info("Adding Insert timestamp")
    return df.withColumn("INSERT_TS", f.lit(datetime.now()))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Saving results to fresh table
# MAGIC
# MAGIC In Bricksflow it is recommended to **work with Hive tables rather than datalake paths**. Hive table has explicit schema which is validated when new rows are inserted into table.
# MAGIC
# MAGIC In the following function, the `bronze_covid.tbl_template_1_mask_usage` table gets (re)created using schema defined in the associated *schema.py* file.

# COMMAND ----------


@data_frame_saver(add_column_insert_ts)
def save_table_bronze_covid_tbl_template_1_mask_usage(df: DataFrame, logger: Logger, table_manager: TableManager):
    # Recreate = remove table and create again
    table_manager.recreate("bronze_covid.tbl_template_1_mask_usage")

    output_table_name = table_manager.get_name("bronze_covid.tbl_template_1_mask_usage")

    logger.info(f"Saving data to table: {output_table_name}")

    (
        df.select("COUNTYFP", "NEVER", "RARELY", "SOMETIMES", "FREQUENTLY", "ALWAYS", "INSERT_TS")
        .write.option("partitionOverwriteMode", "dynamic")
        .insertInto(output_table_name)
    )

    logger.info(f"Data successfully saved to: {output_table_name}")
