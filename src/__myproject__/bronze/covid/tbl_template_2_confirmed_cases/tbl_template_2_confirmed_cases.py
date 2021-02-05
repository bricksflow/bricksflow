# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #2: Tables over files

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver, tableParams
from datalakebundle.table.TableManager import TableManager

# COMMAND ----------


@dataFrameLoader(tableParams("bronze_covid.tbl_template_2_confirmed_cases").source_csv_path, display=False)
def read_csv_covid_confirmed_usafacts(source_csv_path: str, spark: SparkSession, logger: Logger):
    logger.info(f"Reading CSV from source path: `{source_csv_path}`.")
    return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(source_csv_path).limit(10)  # only for test


# COMMAND ----------


@transformation(read_csv_covid_confirmed_usafacts, display=True)
def rename_columns(df: DataFrame):
    return df.withColumnRenamed("County Name", "County_Name")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Working with dataframes outside the notebook function
# MAGIC
# MAGIC While debugging, you can also work directly with dataframes produced by specific notebook function. The dataframe name always follows the `[function name]_df` format.

# COMMAND ----------

display(rename_columns_df)

# COMMAND ----------

print(rename_columns_df.printSchema())

# COMMAND ----------

# MAGIC %md #### Appending data to existing table

# COMMAND ----------


@dataFrameSaver(rename_columns)
def save_table_bronze_covid_tbl_template_2_confirmed_cases(df: DataFrame, logger: Logger, table_manager: TableManager):
    output_table_name = table_manager.getName("bronze_covid.tbl_template_2_confirmed_cases")
    schema = table_manager.getConfig("bronze_covid.tbl_template_2_confirmed_cases").schema

    table_manager.createIfNotExists(
        "bronze_covid.tbl_template_2_confirmed_cases"
    )  # make sure, that the table is created only if not exist yet (first run of the notebook)

    logger.info(f"Saving data to table: {output_table_name}")

    df.select([field.name for field in schema.fields]).write.option("partitionOverwriteMode", "dynamic").insertInto(output_table_name)
