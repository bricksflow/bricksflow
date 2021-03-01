# Databricks notebook source
# MAGIC %md
# MAGIC # Features Customer

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------
from datalakebundle.table.config.tableParams import tableParams
from pyspark.sql import functions as f
from datetime import datetime
from logging import Logger
from pyspark.dbutils import DBUtils  # enables to use Datbricks dbutils within functions
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver, notebookFunction
from datalakebundle.table.TableManager import TableManager
from __myproject__.gold.features.feature import feature

# COMMAND ----------

# MAGIC %md #### Features

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

@feature(read_csv_mask_usage, description="My super feature")
def feature_test(df: DataFrame):
    return df.select(f.col("id", "value"))
