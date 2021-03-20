# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Sample notebook #2: Configuration, appending new data
# MAGIC
# MAGIC In this notebook, you will learn how to **use and change configuration parameters**.

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import data_frame_loader, transformation, data_frame_saver, notebook_function, table_params
from datalakebundle.table.TableManager import TableManager
from __myproject__.bronze.covid.tbl_template_2_confirmed_cases.csv_schema import get_schema as get_csv_schema

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StringType, StructField

# COMMAND ----------


@data_frame_loader(table_params("bronze_covid.tbl_template_2_confirmed_cases").source_csv_path, display=False)
def read_csv_covid_confirmed_usafacts(source_csv_path: str, spark: SparkSession, logger: Logger):
    logger.info(f"Reading CSV from source path: `{source_csv_path}`.")
    return spark.read.format("csv").option("header", "true").schema(get_csv_schema()).load(source_csv_path).limit(10)  # only for test


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Passing configuration to notebook function
# MAGIC
# MAGIC The following function uses the `"%datalake.basePath%"` config parameter. To change it:
# MAGIC
# MAGIC 1. [Setup your local development environment](https://datasentics.github.io/ai-platform-docs/data-pipelines-workflow/local-project-setup/)
# MAGIC 1. Edit the `src/__myproject__/_config/config.yaml` file on your local machine
# MAGIC 1. Deploy changes back to Databricks by using the `console dbx:deploy` command.

# COMMAND ----------


@notebook_function("%datalake.base_path%", read_csv_covid_confirmed_usafacts)
def add_parameter_from_config(base_path, df: DataFrame, logger: Logger):
    logger.info(f"Datalake base path: {base_path}")


# COMMAND ----------


@transformation(read_csv_covid_confirmed_usafacts, display=True)
def rename_columns(df: DataFrame):
    return df.withColumnRenamed("County Name", "County_Name")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Working with dataframes outside notebook function
# MAGIC
# MAGIC While debugging, you can also work directly with dataframes produced by specific notebook function. The dataframe name always follows the `[function name]_df` format.

# COMMAND ----------

display(rename_columns_df)  # noqa: F821

# COMMAND ----------

print(rename_columns_df.printSchema())  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Appending data to existing table
# MAGIC
# MAGIC In the following function, `table_manager.create_if_not_exists("bronze_covid.tbl_template_2_confirmed_cases")` is used to make sure that the table always exists.

# COMMAND ----------


@data_frame_saver(rename_columns)
def save_table_bronze_covid_tbl_template_2_confirmed_cases(df: DataFrame, logger: Logger, table_manager: TableManager):
    output_table_name = table_manager.get_name("bronze_covid.tbl_template_2_confirmed_cases")
    schema = table_manager.get_config("bronze_covid.tbl_template_2_confirmed_cases").schema

    # make sure, that the table is created only if not exist yet (first run of the notebook)
    table_manager.create_if_not_exists("bronze_covid.tbl_template_2_confirmed_cases")

    logger.info(f"Saving data to table: {output_table_name}")

    df.select([field.name for field in schema.fields]).write.option("partitionOverwriteMode", "dynamic").insertInto(output_table_name)
