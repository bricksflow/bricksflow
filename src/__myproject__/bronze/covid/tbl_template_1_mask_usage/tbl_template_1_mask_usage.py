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
from pyspark.sql import functions as F
from logging import Logger
from datalakebundle.table.TableManager import TableManager
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from databricksbundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver, tableParams
from datalakebundle.table.TableNames import TableNames

# COMMAND ----------

@dataFrameLoader(
    tableParams('bronze_covid.tbl_template_1_mask_usage').source_csv_path,
    display=False
)
def read_csv_mask_usage(source_csv_path: str, spark: SparkSession, logger: Logger):
    logger.info(f"Reading CSV from source path: `{source_csv_path}`.")
    return (
        spark
            .read
            .format('csv')
            .option('header', 'true')
            .option('inferSchema', 'true') # Tip: it might be better idea to define schema!
            .load(source_csv_path)
            .limit(10) # only for test
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### @decorators
# MAGIC Did you notice that peace of code above a function starting with "@". It`s a standard python element called _decorator_. Bricksflow uses decorators to enable software engineering approaches while using advantage of interactive notebook. Run a function without explicitly calling it - simulates interactive cell and allows to run as a script without any modification. It is possible to generate Lineage documenation based on order of transformations and other things and many in the future.
# MAGIC - *@dataFrameLoader* - use when loading table or data from source. Accepts varibles from config and returns dataframe.
# MAGIC - *@transformation* - use for any kind of dataframe transformation/step. You probably use many of those. Accepts Input dataframe and varibles from config, Returns dataframe.
# MAGIC - *@dataFrameSaver* - use when saving dataframe to a table. Accepts only Input dataframe and varibles from config.
# MAGIC - *@notebookFunction* - use when running any other Python code like - Mlflow, Widgets, Secrets,...
# MAGIC 
# MAGIC #### Decorators parameters
# MAGIC It is possible to define some functionality by decorates. You have this possibilities:
# MAGIC - Variables from config -> see section _Define param in config.yaml_ bellow 
# MAGIC - `display=True/False`
# MAGIC   Do you use display(df) function to show content of a dataframe? This parameter is exactly the same. By using it as decorator param we are able to easily deactivate it in production where it is not necessary. Set the parameter to True to show data preview or False to skip preview.
# MAGIC   
# MAGIC   <img src="https://github.com/richardcerny/bricksflow/raw/rc-bricksflow2.1/docs/img/display_true.png?raw=true" width=800/>
# MAGIC   
# MAGIC   
# MAGIC 

# COMMAND ----------

# MAGIC %md ### Set parameter display=True to show results in this cell

# COMMAND ----------

@transformation(read_csv_mask_usage, display=True)
def add_column_insert_ts(df: DataFrame, logger: Logger):
    logger.info("Adding Insert timestamp")
    return df.withColumn('INSERT_TS', F.lit(datetime.now()))
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passing dataframe between functions
# MAGIC Normally you would pass dataframes between tranformation like this:
# MAGIC ```
# MAGIC df_1 = df2.select('xxx',...)
# MAGIC df2 = df3.withColumn(...
# MAGIC df3.write...
# MAGIC ```
# MAGIC *Bricksflow does it a bit differently!*
# MAGIC 
# MAGIC Basically you use name of original function and place it as an input parameter to following(or any other) function`s @decorator. Thanks to this you are able to easilly navigate between functions in your IDE.
# MAGIC See bellow how to pass dataframe from one function to another.
# MAGIC 
# MAGIC ![Passing dataframe between functions](https://github.com/richardcerny/bricksflow/raw/rc-bricksflow2.1/docs/img/df_passing.png)
# MAGIC 
# MAGIC You can see this in acion accross this notebook.

# COMMAND ----------

@dataFrameSaver(add_column_insert_ts)
def save_table_bronze_covid_tbl_template_1_mask_usage(df: DataFrame, logger: Logger, tableNames: TableNames,  tableManager: TableManager):
    
    # Recreate = remove table and create again
    tableManager.recreate('bronze_covid.tbl_template_1_mask_usage')
    
    outputTableName = tableNames.getByAlias('bronze_covid.tbl_template_1_mask_usage')
    logger.info(f"Saving data to table: {outputTableName}")
    (
        df
            .select(
                'COUNTYFP',
                'NEVER',
                'RARELY',
                'SOMETIMES',
                'FREQUENTLY',
                'ALWAYS',
                'INSERT_TS'
            )
            .write
            .option('partitionOverwriteMode', 'dynamic')
            .insertInto(outputTableName)
    )
    logger.info(f"Data successfully saved to: {outputTableName}")