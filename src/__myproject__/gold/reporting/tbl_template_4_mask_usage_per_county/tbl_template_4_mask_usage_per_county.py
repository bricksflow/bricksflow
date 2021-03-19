# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #4: Joining multiple dataframes
# MAGIC

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f
from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import data_frame_loader, transformation, data_frame_saver
from datalakebundle.table.TableManager import TableManager

# COMMAND ----------


@data_frame_loader(display=True)
def read_bronze_covid_tbl_template_2_confirmed_case(spark: SparkSession, table_manager: TableManager):
    return (
        spark.read.table(table_manager.get_name("bronze_covid.tbl_template_2_confirmed_cases"))
        .select("countyFIPS", "County_Name", "State", "stateFIPS")
        .dropDuplicates()
    )


# COMMAND ----------


@data_frame_loader(display=True)
def read_table_silver_covid_tbl_template_3_mask_usage(spark: SparkSession, table_manager: TableManager):
    return (
        spark.read.table(table_manager.get_name("silver_covid.tbl_template_3_mask_usage"))
        .limit(10)  # only for test
        .withColumn("EXECUTE_DATE", f.to_date(f.col("EXECUTE_DATETIME")))
    )


# COMMAND ----------

# MAGIC %md #### Joining multiple dataframes in `@transformation`

# COMMAND ----------


@transformation(read_bronze_covid_tbl_template_2_confirmed_case, read_table_silver_covid_tbl_template_3_mask_usage, display=True)
def join_covid_datasets(df1: DataFrame, df2: DataFrame):
    return df1.join(df2, df1.countyFIPS == df2.COUNTYFP, how="right")


# COMMAND ----------


@transformation(join_covid_datasets, display=False)
def agg_avg_mask_usage_per_county(df: DataFrame):
    return df.groupBy("EXECUTE_DATE", "County_Name").agg(
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


@data_frame_saver(standardize_dataset)
def save_table_gold_tbl_template_4_mask_usage_per_count(df: DataFrame, logger: Logger, table_manager: TableManager):
    output_table_name = table_manager.get_name("gold_reporting.tbl_template_4_mask_usage_per_county")
    table_manager.recreate("gold_reporting.tbl_template_4_mask_usage_per_county")
    logger.info(f"Saving data to table: {output_table_name}")
    (
        df.select(
            "EXECUTE_DATE",
            "COUNTY_NAME",
            "AVG_NEVER",
            "AVG_RARELY",
            "AVG_SOMETIMES",
            "AVG_FREQUENTLY",
            "AVG_ALWAYS",
        )
        .write.option("partitionOverwriteMode", "dynamic")
        .insertInto(output_table_name)
    )
