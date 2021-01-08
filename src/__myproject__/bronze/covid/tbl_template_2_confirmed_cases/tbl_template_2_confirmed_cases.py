# Databricks notebook source
# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from logging import Logger
from datalakebundle.table.TableManager import TableManager
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from databricksbundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver
from datalakebundle.table.TableNames import TableNames

# COMMAND ----------

@dataFrameLoader(display=False)
def read_csv_covid_confirmed_usafacts(spark: SparkSession):
    return (
        spark
            .read
            .format('csv')
            .option('header', 'true')
            .option('inferSchema', 'true')
            .load('dbfs:/databricks-datasets/COVID/USAFacts/covid_confirmed_usafacts.csv')
    )

# COMMAND ----------

@transformation(read_csv_covid_confirmed_usafacts, display=True)
def rename_columns(df: DataFrame):
    return (
        df
            .withColumnRenamed('County Name', 'County_Name')
    )

# COMMAND ----------

@dataFrameSaver(rename_columns)
def save_confirmed_cases(df: DataFrame, logger: Logger, tableNames: TableNames, tableManager: TableManager):
    schema = tableManager.getSchema('bronze_covid.tbl_template_2_confirmed_cases')

    tableManager.recreate('bronze_covid.tbl_template_2_confirmed_cases')

    outputTableName = tableNames.getByAlias('bronze_covid.tbl_template_2_confirmed_cases')
    logger.info(f"Saving data to table: {outputTableName}")
    (
        df
            .select([field.name for field in schema.fields])
            .write
            .option('partitionOverwriteMode', 'dynamic')
            .insertInto(outputTableName)
    )
