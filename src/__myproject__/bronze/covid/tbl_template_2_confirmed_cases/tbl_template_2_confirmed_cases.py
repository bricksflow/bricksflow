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

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StringType, StructField

schema = StructType(
    [
        StructField("countyFIPS", IntegerType()),
        StructField("County_Name", StringType()),
        StructField("State", StringType()),
        StructField("stateFIPS", IntegerType()),
        StructField("1/22/20", IntegerType()),
        StructField("1/23/20", IntegerType()),
        StructField("1/24/20", IntegerType()),
        StructField("1/25/20", IntegerType()),
        StructField("1/26/20", IntegerType()),
        StructField("1/27/20", IntegerType()),
        StructField("1/28/20", IntegerType()),
        StructField("1/29/20", IntegerType()),
        StructField("1/30/20", IntegerType()),
        StructField("1/31/20", IntegerType()),
        StructField("2/1/20", IntegerType()),
        StructField("2/2/20", IntegerType()),
        StructField("2/3/20", IntegerType()),
        StructField("2/4/20", IntegerType()),
        StructField("2/5/20", IntegerType()),
        StructField("2/6/20", IntegerType()),
        StructField("2/7/20", IntegerType()),
        StructField("2/8/20", IntegerType()),
        StructField("2/9/20", IntegerType()),
        StructField("2/10/20", IntegerType()),
        StructField("2/11/20", IntegerType()),
        StructField("2/12/20", IntegerType()),
        StructField("2/13/20", IntegerType()),
        StructField("2/14/20", IntegerType()),
        StructField("2/15/20", IntegerType()),
        StructField("2/16/20", IntegerType()),
        StructField("2/17/20", IntegerType()),
        StructField("2/18/20", IntegerType()),
        StructField("2/19/20", IntegerType()),
        StructField("2/20/20", IntegerType()),
        StructField("2/21/20", IntegerType()),
        StructField("2/22/20", IntegerType()),
        StructField("2/23/20", IntegerType()),
        StructField("2/24/20", IntegerType()),
        StructField("2/25/20", IntegerType()),
        StructField("2/26/20", IntegerType()),
        StructField("2/27/20", IntegerType()),
        StructField("2/28/20", IntegerType()),
        StructField("2/29/20", IntegerType()),
        StructField("3/1/20", IntegerType()),
        StructField("3/2/20", IntegerType()),
        StructField("3/3/20", IntegerType()),
        StructField("3/4/20", IntegerType()),
        StructField("3/5/20", IntegerType()),
        StructField("3/6/20", IntegerType()),
        StructField("3/7/20", IntegerType()),
        StructField("3/8/20", IntegerType()),
        StructField("3/9/20", IntegerType()),
        StructField("3/10/20", IntegerType()),
        StructField("3/11/20", IntegerType()),
        StructField("3/12/20", IntegerType()),
        StructField("3/13/20", IntegerType()),
        StructField("3/14/20", IntegerType()),
        StructField("3/15/20", IntegerType()),
        StructField("3/16/20", IntegerType()),
        StructField("3/17/20", IntegerType()),
        StructField("3/18/20", IntegerType()),
        StructField("3/19/20", IntegerType()),
        StructField("3/20/20", IntegerType()),
        StructField("3/21/20", IntegerType()),
        StructField("3/22/20", IntegerType()),
        StructField("3/23/20", IntegerType()),
        StructField("3/24/20", IntegerType()),
        StructField("3/25/20", IntegerType()),
        StructField("3/26/20", IntegerType()),
        StructField("3/27/20", IntegerType()),
        StructField("3/28/20", IntegerType()),
        StructField("3/29/20", IntegerType()),
        StructField("3/30/20", IntegerType()),
        StructField("3/31/20", IntegerType()),
        StructField("4/1/20", IntegerType()),
        StructField("4/2/20", IntegerType()),
        StructField("4/3/20", IntegerType()),
        StructField("4/4/20", IntegerType()),
        StructField("4/5/20", IntegerType()),
        StructField("4/6/20", IntegerType()),
        StructField("4/7/20", IntegerType()),
        StructField("4/8/20", IntegerType()),
        StructField("4/9/20", IntegerType()),
        StructField("4/10/20", IntegerType()),
        StructField("4/11/20", IntegerType()),
        StructField("4/12/20", IntegerType()),
        StructField("4/13/20", IntegerType()),
        StructField("4/14/20", IntegerType()),
        StructField("4/15/20", IntegerType()),
        StructField("4/16/20", IntegerType()),
        StructField("4/17/20", IntegerType()),
        StructField("4/18/20", IntegerType()),
        StructField("4/19/20", IntegerType()),
        StructField("4/20/20", IntegerType()),
        StructField("4/21/20", IntegerType()),
        StructField("4/22/20", IntegerType()),
        StructField("4/23/20", IntegerType()),
        StructField("4/24/20", IntegerType()),
        StructField("4/25/20", IntegerType()),
        StructField("4/26/20", IntegerType()),
        StructField("4/27/20", IntegerType()),
        StructField("4/28/20", IntegerType()),
        StructField("4/29/20", IntegerType()),
        StructField("4/30/20", IntegerType()),
        StructField("5/1/20", IntegerType()),
        StructField("5/2/20", IntegerType()),
        StructField("5/3/20", IntegerType()),
        StructField("5/4/20", IntegerType()),
        StructField("5/5/20", IntegerType()),
        StructField("5/6/20", IntegerType()),
        StructField("5/7/20", IntegerType()),
        StructField("5/8/20", IntegerType()),
        StructField("5/9/20", IntegerType()),
        StructField("5/10/20", IntegerType()),
        StructField("5/11/20", IntegerType()),
        StructField("5/12/20", IntegerType()),
        StructField("5/13/20", IntegerType()),
        StructField("5/14/20", IntegerType()),
        StructField("5/15/20", IntegerType()),
        StructField("5/16/20", IntegerType()),
        StructField("5/17/20", IntegerType()),
        StructField("5/18/20", IntegerType()),
        StructField("5/19/20", IntegerType()),
        StructField("5/20/20", IntegerType()),
        StructField("5/21/20", IntegerType()),
        StructField("5/22/20", IntegerType()),
        StructField("5/23/20", IntegerType()),
        StructField("5/24/20", IntegerType()),
        StructField("5/25/20", IntegerType()),
        StructField("5/26/20", IntegerType()),
        StructField("5/27/20", IntegerType()),
        StructField("5/28/20", IntegerType()),
        StructField("5/29/20", IntegerType()),
        StructField("5/30/20", IntegerType()),
        StructField("5/31/20", IntegerType()),
        StructField("6/1/20", IntegerType()),
        StructField("6/2/20", IntegerType()),
        StructField("6/3/20", IntegerType()),
        StructField("6/4/20", IntegerType()),
        StructField("6/5/20", IntegerType()),
        StructField("6/6/20", IntegerType()),
        StructField("6/7/20", IntegerType()),
        StructField("6/8/20", IntegerType()),
        StructField("6/9/20", IntegerType()),
        StructField("6/10/20", IntegerType()),
        StructField("6/11/20", IntegerType()),
        StructField("6/12/20", IntegerType()),
        StructField("6/13/20", IntegerType()),
        StructField("6/14/20", IntegerType()),
        StructField("6/15/20", IntegerType()),
        StructField("6/16/20", IntegerType()),
        StructField("6/17/20", IntegerType()),
        StructField("6/18/20", IntegerType()),
        StructField("6/19/20", IntegerType()),
        StructField("6/20/20", IntegerType()),
        StructField("6/21/20", IntegerType()),
        StructField("6/22/20", IntegerType()),
        StructField("6/23/20", IntegerType()),
        StructField("6/24/20", IntegerType()),
        StructField("6/25/20", IntegerType()),
        StructField("6/26/20", IntegerType()),
        StructField("6/27/20", IntegerType()),
        StructField("6/28/20", IntegerType()),
        StructField("6/29/20", IntegerType()),
        StructField("6/30/20", IntegerType()),
        StructField("7/1/20", IntegerType()),
        StructField("7/2/20", IntegerType()),
        StructField("7/3/20", IntegerType()),
        StructField("7/4/20", IntegerType()),
        StructField("7/5/20", IntegerType()),
        StructField("7/6/20", IntegerType()),
        StructField("7/7/20", IntegerType()),
        StructField("7/8/20", IntegerType()),
        StructField("7/9/20", IntegerType()),
        StructField("7/10/20", IntegerType()),
        StructField("7/11/20", IntegerType()),
        StructField("7/12/20", IntegerType()),
        StructField("7/13/20", IntegerType()),
        StructField("7/14/20", IntegerType()),
        StructField("7/15/20", IntegerType()),
        StructField("7/16/20", IntegerType()),
        StructField("7/17/20", IntegerType()),
        StructField("7/18/20", IntegerType()),
        StructField("7/19/20", IntegerType()),
        StructField("7/20/20", IntegerType()),
        StructField("7/21/20", IntegerType()),
        StructField("7/22/20", IntegerType()),
        StructField("7/23/20", IntegerType()),
        StructField("7/24/20", IntegerType()),
        StructField("7/25/20", IntegerType()),
        StructField("7/26/20", IntegerType()),
        StructField("7/27/20", IntegerType()),
        StructField("7/28/20", IntegerType()),
        StructField("7/29/20", IntegerType()),
        StructField("7/30/20", IntegerType()),
        StructField("7/31/20", IntegerType()),
        StructField("8/1/20", IntegerType()),
        StructField("8/2/20", IntegerType()),
        StructField("8/3/20", IntegerType()),
        StructField("8/4/20", IntegerType()),
        StructField("8/5/20", IntegerType()),
        StructField("8/6/20", IntegerType()),
        StructField("8/7/20", IntegerType()),
        StructField("8/8/20", IntegerType()),
        StructField("8/9/20", IntegerType()),
        StructField("8/10/20", IntegerType()),
        StructField("8/11/20", IntegerType()),
        StructField("8/12/20", IntegerType()),
        StructField("8/13/20", IntegerType()),
        StructField("8/14/20", IntegerType()),
        StructField("8/15/20", IntegerType()),
        StructField("8/16/20", IntegerType()),
        StructField("8/17/20", IntegerType()),
        StructField("8/18/20", IntegerType()),
        StructField("8/19/20", IntegerType()),
        StructField("8/20/20", IntegerType()),
        StructField("8/21/20", IntegerType()),
        StructField("8/22/20", IntegerType()),
        StructField("8/23/20", IntegerType()),
        StructField("8/24/20", IntegerType()),
        StructField("8/25/20", IntegerType()),
        StructField("8/26/20", IntegerType()),
        StructField("8/27/20", IntegerType()),
        StructField("8/28/20", IntegerType()),
        StructField("8/29/20", IntegerType()),
        StructField("8/30/20", IntegerType()),
        StructField("8/31/20", IntegerType()),
        StructField("9/1/20", IntegerType()),
        StructField("9/2/20", IntegerType()),
        StructField("9/3/20", IntegerType()),
        StructField("9/4/20", IntegerType()),
        StructField("9/5/20", IntegerType()),
        StructField("9/6/20", IntegerType()),
        StructField("9/7/20", IntegerType()),
        StructField("9/8/20", IntegerType()),
        StructField("9/9/20", IntegerType()),
        StructField("9/10/20", IntegerType()),
        StructField("9/11/20", IntegerType()),
        StructField("9/12/20", IntegerType()),
    ]
)

# COMMAND ----------


@data_frame_loader(table_params("bronze_covid.tbl_template_2_confirmed_cases").source_csv_path, display=False)
def read_csv_covid_confirmed_usafacts(source_csv_path: str, spark: SparkSession, logger: Logger):
    logger.info(f"Reading CSV from source path: `{source_csv_path}`.")
    return spark.read.format("csv").option("header", "true").schema(schema).load(source_csv_path).limit(10)  # only for test


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
