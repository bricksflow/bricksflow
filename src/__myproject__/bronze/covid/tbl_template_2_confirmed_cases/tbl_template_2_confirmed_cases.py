# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-bricksflow2.1/docs/img/databricks_icon.png?raw=true" width=100/>
# MAGIC # Bricksflow example 2.
# MAGIC 
# MAGIC ## Tables over files
# MAGIC You know parquet or delta, right. It is great format to store data. But for Data analyst it might be difficult to find and load these files for Blob Storage or S3. Because of that we try to keep everything in tables that are stored in Hive Metastore (=this is list of database and tables that show after opening Data icon on the left). Data analyst can just do quick analysys with SQL quickly and doesn't need to bother about loading paruqet format.
# MAGIC 
# MAGIC 
# MAGIC ## Datalake structure
# MAGIC There are many ways how to create database and table names. You can see two approaches in DataSentics. They are pretty similar just the naming is used  bit differently. As the common concept used by Datalake is Bronze, Silver, Gold. Before this become common we used sometinhg similar Raw, Parsed, Cleansed.
# MAGIC 
# MAGIC **Key concept of Datalake created by DataSentics:**
# MAGIC - according to database and table name it is possible to find raw data in Storage and notebook/script that creates it and other way arround
# MAGIC - keep all source files
# MAGIC - ONE notebook/script creates only ONE table
# MAGIC - tables are defined in config (not hardcoded in the code)
# MAGIC - table schema is tight to the script/notebook
# MAGIC - data is written to a table (not to a file)
# MAGIC - ...
# MAGIC 
# MAGIC ## Datalake structure in Bricksflow
# MAGIC Bricksflow uses so called Datalake bundle to maintain all tables within a config so you always now which tables are in use.
# MAGIC By default it requires just a table name and it resolves the path where to save data automatically.
# MAGIC 
# MAGIC ##### Example of config.yaml
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-bricksflow2.1/docs/img/datalake_config.png?raw=true" width=1200/>
# MAGIC 
# MAGIC Non-default setting:
# MAGIC - If you want to adjust Storage path resolver you can find it here: ``\src\__myproject__\_config\bundles\datalakebundle.yaml`. It uses simple python find function to split the table names according "_".
# MAGIC - If you need exact path you can use variable `targetPath` to save a table to desired location. See commented example in config.yaml in picture above.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### 1. Common Datalake layers Bronze, Silver, Gold
# MAGIC Todo Image
# MAGIC 
# MAGIC ### 2. ADAP Layers dataoneoff, dataregular, solutions
# MAGIC Todo Image
# MAGIC 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other Config variables
# MAGIC You might have noticed some variables which were used in the functions above like `source_csv_path` and `outputTableName`. This variables/parameters are gathered from config file that is editable only from Git/Local. It is not possible to change config within Databricks (yet).
# MAGIC 
# MAGIC There are some general config parameters
# MAGIC 
# MAGIC 
# MAGIC #### Basic parameters
# MAGIC Common parameters accessible anywhere and passed through @decorator
# MAGIC 
# MAGIC **Define param in config.yaml**
# MAGIC ```
# MAGIC parameters:
# MAGIC   myparameter:
# MAGIC     myvalue: 'This is a sample string config value'
# MAGIC ```
# MAGIC **Use param in code**
# MAGIC ```
# MAGIC @dataFrameLoader("%myparameter.myvalue%", display=True)
# MAGIC def tbl_raw_trace_event(my_value_from_config: str, spark: SparkSession)
# MAGIC    print(my_value_from_config)
# MAGIC    ...
# MAGIC ```

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from logging import Logger
from datalakebundle.table.TableManager import TableManager
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from databricksbundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver, tableParams
from datalakebundle.table.TableNames import TableNames

# COMMAND ----------

@dataFrameLoader(tableParams('bronze_covid.tbl_template_2_confirmed_cases').source_csv_path, display=False)
def read_csv_covid_confirmed_usafacts(source_csv_path: str, spark: SparkSession, logger: Logger):
    logger.info(f"Reading CSV from source path: `{source_csv_path}`.")
    return (
        spark
            .read
            .format('csv')
            .option('header', 'true')
            .option('inferSchema', 'true')
            .load(source_csv_path)
            .limit(10) # only for test
    )

# COMMAND ----------

@transformation(read_csv_covid_confirmed_usafacts, display=True)
def rename_columns(df: DataFrame):
    return (
        df
            .withColumnRenamed('County Name', 'County_Name')
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Table schema
# MAGIC Each table defined in config must be tight with its schema. So you need to create schema of a table if you need to create it.
# MAGIC But don`t worry creating a schema is one of the last step you need to do while creating pipeline. Use display instead to show temporary results. Once you are happy with the result and you know exactly which columns are necessay then create a  schema.
# MAGIC 
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-bricksflow2.1/docs/img/table_schema.png?raw=true"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC You can get your defined schema from config using this command example:
# MAGIC `schema = tableManager.getSchema('bronze_covid.tbl_template_2_confirmed_cases')`
# MAGIC 
# MAGIC 
# MAGIC TODO add schema generator to Bricksflow 
# MAGIC print(createPysparkSchema(add_parameter_from_config_df.schema))

# COMMAND ----------

# MAGIC %md
# MAGIC TIP: Did you know that each function returns a dataframe. So if you need to test someting or get a schema for example, you can quickly do discovery on the dataframe produced by function. Just by calling `[function name].result`. It returns standard Spark Dataframe so you can test code before you wrap it into a function.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-bricksflow2.1/docs/img/function_returns_df.png?raw=true"/>
# MAGIC 

# COMMAND ----------

# MAGIC %md ### Example function result

# COMMAND ----------

df = rename_columns.result

# COMMAND ----------

print(df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table Manager
# MAGIC You have your table and schema defined in config, so you can now do number of thing with it and Table Manager will take care of the thing underhood - saves data to right path, solves different environments.
# MAGIC 
# MAGIC It gives you these options which you can use:
# MAGIC - create -> create table (if already exists it throws an error)
# MAGIC - delete -> delete table
# MAGIC - recreate -> deleta and create a table (old data removed - also Delta log is removed => no Time travel)
# MAGIC - exists -> check table existence and returns True/False
# MAGIC - getSchema -> returns defined schema
# MAGIC 
# MAGIC Check examples of using these commands in cells bellow.

# COMMAND ----------

# MAGIC %md ### a) You want create a table once and than append data use 'exists' & 'create'

# COMMAND ----------

@dataFrameSaver(rename_columns)
def save_table_ronze_covid_tbl_template_2_confirmed_cases(df: DataFrame, logger: Logger, tableNames: TableNames, tableManager: TableManager):
    outputTableName = tableNames.getByAlias('bronze_covid.tbl_template_2_confirmed_cases')
    
    # TableManager
    # get defined schema
    schema = tableManager.getSchema('bronze_covid.tbl_template_2_confirmed_cases')
    
    # Example of delete command
    #tableManager.delete('bronze_covid.tbl_template_2_confirmed_cases')

    if tableManager.exists('bronze_covid.tbl_template_2_confirmed_cases'):
        logger.info(f"Table {outputTableName} exists. Appending...")
    else:
        tableManager.create('bronze_covid.tbl_template_2_confirmed_cases')
        
    logger.info(f"Saving data to table: {outputTableName}")
    (
        df
            .select([field.name for field in schema.fields]) # Usage of schema from getSchema
            .write
            .option('partitionOverwriteMode', 'dynamic')
            .insertInto(outputTableName)
    )

# COMMAND ----------

# MAGIC %md ### b) You want a table to be created each run from scratch - use 'recreate'

# COMMAND ----------

@dataFrameSaver(rename_columns)
def save_table_ronze_covid_tbl_template_2_confirmed_cases(df: DataFrame, logger: Logger, tableNames: TableNames, tableManager: TableManager):
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
