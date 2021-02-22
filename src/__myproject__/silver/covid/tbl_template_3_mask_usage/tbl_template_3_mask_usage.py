# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #3: Widgets

# COMMAND ----------

# MAGIC %md
# MAGIC #### Widgets
# MAGIC Many people love using [Databricks widgets](https://docs.databricks.com/notebooks/widgets.html) to parametrize notebooks. To use widgets in Bricksflow, you should put them into a `@notebookFunction`.
# MAGIC
# MAGIC <img src="https://github.com/bricksflow/bricksflow/raw/master/docs/widgets.png?raw=true" width=1000/>
# MAGIC
# MAGIC Don't forget to check  or run command `dbutils.widgets.help()` to see options you have while working with widget.

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f
from datetime import datetime
from logging import Logger
from pyspark.dbutils import DBUtils  # enables to use Datbricks dbutils within functions
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver, notebookFunction
from datalakebundle.table.TableManager import TableManager

# COMMAND ----------

# MAGIC %md #### Creating a widget

# COMMAND ----------


@notebookFunction()
def create_input_widgets(dbutils: DBUtils):
    dbutils.widgets.dropdown("base_year", "2020", ["2018", "2019", "2020", "2021"], "Base year")


# COMMAND ----------


@dataFrameLoader(display=True)
def read_table_bronze_covid_tbl_template_1_mask_usage(spark: SparkSession, table_manager: TableManager, logger: Logger, dbutils: DBUtils):
    base_year = dbutils.widgets.get("base_year")

    logger.info(f"Using base year: {base_year}")

    df = spark.read.table(table_manager.getName("bronze_covid.tbl_template_1_mask_usage"))

    return df.filter(f.col("INSERT_TS") >= base_year)


# COMMAND ----------


@transformation(read_table_bronze_covid_tbl_template_1_mask_usage, display=False)
def add_execution_datetime(df: DataFrame):
    return df.withColumn("EXECUTE_DATETIME", f.lit(datetime.now()))


# COMMAND ----------


@dataFrameSaver(add_execution_datetime)
def save_table_silver_covid_tbl_template_3_mask_usage(df: DataFrame, logger: Logger, table_manager: TableManager):
    output_table_name = table_manager.getName("silver_covid.tbl_template_3_mask_usage")

    table_manager.recreate("silver_covid.tbl_template_3_mask_usage")

    logger.info(f"Saving data to table: {output_table_name}")
    (
        df.select(
            "COUNTYFP",
            "NEVER",
            "RARELY",
            "SOMETIMES",
            "FREQUENTLY",
            "ALWAYS",
            "EXECUTE_DATETIME",
        )
        .write.option("partitionOverwriteMode", "dynamic")
        .insertInto(output_table_name)
    )


# COMMAND ----------

# MAGIC %md ### Removing all widgets

# COMMAND ----------

# dbutils.widgets.removeAll()
