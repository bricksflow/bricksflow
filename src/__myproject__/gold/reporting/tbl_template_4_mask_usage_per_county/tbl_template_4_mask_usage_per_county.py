# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-bricksflow2.1/docs/img/databricks_icon.png?raw=true" width=100/>
# MAGIC # Bricksflow example 4.
# MAGIC
# MAGIC ## Productionalizing notebook in Bricksflow
# MAGIC It always takse some time to productionalize notebook.
# MAGIC What is usually necesary to do:
# MAGIC - cleaning a code from testing part
# MAGIC - comments some part of code
# MAGIC - all code is in functions
# MAGIC - remove unnecesary comments
# MAGIC - resolve ToDos
# MAGIC - replace hardcoded variable with config parameters
# MAGIC - test that it still works the same after clean up
# MAGIC - use linting tools (pylint, black, flake8)
# MAGIC - ...

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f
from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver
from datalakebundle.table.TableManager import TableManager

# COMMAND ----------


@dataFrameLoader(display=True)
def read_bronze_covid_tbl_template_2_confirmed_case(spark: SparkSession, table_manager: TableManager):
    return (
        spark.read.table(table_manager.getName("bronze_covid.tbl_template_2_confirmed_cases"))
        .select("countyFIPS", "County_Name", "State", "stateFIPS")
        .dropDuplicates()
    )


# COMMAND ----------


@dataFrameLoader(display=True)
def read_table_silver_covid_tbl_template_3_mask_usage(spark: SparkSession, table_manager: TableManager):
    return (
        spark.read.table(table_manager.getName("silver_covid.tbl_template_3_mask_usage"))
        .limit(10)  # only for test
        .withColumn("EXECUTE_DATE", f.to_date(f.col("EXECUTE_DATETIME")))
    )


# COMMAND ----------

# MAGIC %md ### How to join more dataframes using @transformation?

# COMMAND ----------


@transformation(read_bronze_covid_tbl_template_2_confirmed_case, read_table_silver_covid_tbl_template_3_mask_usage, display=True)
def join_covid_datasets(df1: DataFrame, df2: DataFrame):
    return df1.join(df2, df1.countyFIPS == df2.COUNTYFP, how="right")


# COMMAND ----------


@transformation(join_covid_datasets, display=False)
def agg_avg_mask_usage_per_county(df: DataFrame):
    return df.groupBy("EXECUTE_DATE", "County_Name", "CONFIG_YAML_PARAMETER").agg(
        f.avg("NEVER").alias("AVG_NEVER"),
        f.avg("RARELY").alias("AVG_RARELY"),
        f.avg("SOMETIMES").alias("AVG_SOMETIMES"),
        f.avg("FREQUENTLY").alias("AVG_FREQUENTLY"),
        f.avg("ALWAYS").alias("AVG_ALWAYS"),
    )


# COMMAND ----------


@transformation(agg_avg_mask_usage_per_county, display=True)
def standardize_dataset(df: DataFrame):
    return df.withColumnRenamed("County_Name", "COUNTY_NAME")


# COMMAND ----------


@dataFrameSaver(standardize_dataset)
def save_table_gold_tbl_template_4_mask_usage_per_count(df: DataFrame, logger: Logger, table_manager: TableManager):
    output_table_name = table_manager.getName("gold_reporting.tbl_template_4_mask_usage_per_county")
    table_manager.recreate("gold_reporting.tbl_template_4_mask_usage_per_county")
    logger.info(f"Saving data to table: {output_table_name}")
    (
        df.select(
            "EXECUTE_DATE",
            "COUNTY_NAME",
            "CONFIG_YAML_PARAMETER",
            "AVG_NEVER",
            "AVG_RARELY",
            "AVG_SOMETIMES",
            "AVG_FREQUENTLY",
            "AVG_ALWAYS",
        )
        .write.option("partitionOverwriteMode", "dynamic")
        .insertInto(output_table_name)
    )