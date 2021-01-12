# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/databricks_icon.png?raw=true" width=100/> 
# MAGIC # Bricksflow example 1.
# MAGIC 
# MAGIC ## Create new table from CSV
# MAGIC 
# MAGIC This is a very first template notebook that should give you brief overeview how to develop pipeline using Bricksflow.
# MAGIC 
# MAGIC You learn how to organize cells and functions, use `@decorators`, pass variables from `config.yaml`.
# MAGIC 
# MAGIC There are other template notebooks within this project so just look for _template_ notebooks within workspace.
# MAGIC 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Requirements for running this notebook
# MAGIC It is possible to run this demo notebook to see it in action. The datasource is public dataset from Databricks so you should be able to access it.
# MAGIC 
# MAGIC It is expected that following set-up is already configured.
# MAGIC 
# MAGIC ##### Environment variables defined on a cluster
# MAGIC ```
# MAGIC APP_ENV=dev
# MAGIC ```
# MAGIC 
# MAGIC ##### Database `dev_bronze_covid`
# MAGIC 
# MAGIC If you want to run this notebook you need to create it using this command(the cell is prepared bellow):
# MAGIC ```
# MAGIC %sql
# MAGIC create database if not exists dev_bronze_covid
# MAGIC ```
# MAGIC __NOTE:__ Tested on a cluster running Databricks 7.3.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- this cell is only for demo purposes
# MAGIC create database if not exists dev_bronze_covid;
# MAGIC create database if not exists dev_silver_covid;
# MAGIC create database if not exists dev_gold_reporting

# COMMAND ----------

# DBTITLE 1,This command loads Bricksflow framework and its dependencies
# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

# DBTITLE 1,All your imports should be placed up here 
from datetime import datetime
from pyspark.sql import functions as F

from logging import Logger
from datalakebundle.table.TableManager import TableManager
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from databricksbundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver
from datalakebundle.table.TableNames import TableNames

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cells and functions
# MAGIC 
# MAGIC Bricksflow`s best practice to write transformation is by using function per cell approach. Each transformation has its own function and is used in one cell. This sorting of cells and functions significatly improves debuggability of each step and bring other advantages.
# MAGIC 
# MAGIC We try to avoid complex dataframe manipulation within one function. Function name should briefly describe what it does.
# MAGIC 
# MAGIC We are able to create so called *Lineage* that shows all aggregations input/output tables. This is usefull especially for business analysts as they have better idead what is happening.
# MAGIC 
# MAGIC #### Lineage example
# MAGIC 
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/lineage.png?raw=true" width=1200/> 
# MAGIC 

# COMMAND ----------

# Check 
@dataFrameLoader("%datalakebundle.tables%", display=False)
def read_csv_mask_usage(parameters_datalakebundle, spark: SparkSession, logger: Logger):
    source_csv_path = parameters_datalakebundle['bronze_covid.tbl_template_1_mask_usage']['params']['source_csv_path']
    logger.info(f"Reading CSV from source path: `{source_csv_path}`.")
    return (
        spark
            .read
            .format('csv')
            .option('header', 'true')
            .option('inferSchema', 'true') # Tip: it might be better idea to define schema!
            .load(source_csv_path)
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
# MAGIC   <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/display_true.png?raw=true" width=800/> 
# MAGIC   
# MAGIC   
# MAGIC 

# COMMAND ----------

# DBTITLE 1,Set parameter display=True to show results in this cell
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
# MAGIC ![Passing dataframe between functions](https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/df_passing.png)
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