# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/databricks_icon.png?raw=true" width=100/> 
# MAGIC # Bricksflow example 5.
# MAGIC 
# MAGIC Widgets, secrets, notebookFunction
# MAGIC 
# MAGIC ## Widgets
# MAGIC Many people love widgets as they can easilly parametrize their notebook. It is possible to use widget with Bricksflow. Usage is demonstrated in this notebook. Don't forget to check [Widgets documentation](ttps://docs.databricks.com/notebooks/widgets.html) or run command `dbutils.widgets.help()` to see options you have while working with widget.

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

# DBTITLE 1,Databricks widget options
dbutils.widgets.help() # just advice for you :)

# COMMAND ----------

from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from databricksbundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver, notebookFunction
from datalakebundle.table.TableNames import TableNames

from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils # enables to use Datbricks dbutils within functions

# COMMAND ----------

# DBTITLE 1,Create a widget
@notebookFunction()
def create_input_widgets(dbutils: DBUtils):
  dbutils.widgets.text("widget_states", "CA", 'States') # Examples: CA, IL, IN,...
  dbutils.widgets.dropdown("widget_year", '2020', ['2018', '2019', '2020'], 'Example Dropdown Widget') # Examples: CA, IL, IN,...

# COMMAND ----------

# DBTITLE 1,Usage of widget variable
@dataFrameLoader(display=True)
def read_bronze_covid_tbl_template_2_confirmed_case(spark: SparkSession, logger: Logger, tableNames: TableNames, dbutils: DBUtils):
    stateName = dbutils.widgets.get("widget_states")
    logger.info(f"States Widget value: {stateName}")
    return (
        spark
            .read
            .table(tableNames.getByAlias('bronze_covid.tbl_template_2_confirmed_cases'))
            .select('countyFIPS','County_Name', 'State', 'stateFIPS')
            .filter(F.col('State')==stateName) # Widget variable is used
    )

# COMMAND ----------

@transformation(read_bronze_covid_tbl_template_2_confirmed_case, display=True)
def add_year_value(df: DataFrame, logger: Logger, dbutils: DBUtils):
    yearWidget = dbutils.widgets.get("widget_year")
    logger.info(f"Year Widget value: {yearWidget}")
    return (
        df
          .withColumn('WIDGET_YEAR', F.lit(yearWidget))
    )

# COMMAND ----------

# other commands

# COMMAND ----------

# DBTITLE 1,Command to remove all Widget
#dbutils.widgets.removeAll()