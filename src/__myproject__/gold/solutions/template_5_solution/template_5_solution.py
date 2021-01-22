# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-bricksflow2.1/docs/img/databricks_icon.png?raw=true" width=100/>
# MAGIC # Bricksflow example 5.
# MAGIC 
# MAGIC Widgets, secrets, notebookFunction
# MAGIC 
# MAGIC ## Widgets
# MAGIC Many people love widgets as they can easilly parametrize their notebook. It is possible to use widget with Bricksflow. Usage is demonstrated in this notebook. Don't forget to check [Widgets documentation](ttps://docs.databricks.com/notebooks/widgets.html) or run command `dbutils.widgets.help()` to see options you have while working with widget.
# MAGIC 
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-bricksflow2.1/docs/img/widgets.PNG?raw=true" width=1000/>

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from databricksbundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver, notebookFunction
from datalakebundle.table.TableNames import TableNames

from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils # enables to use Datbricks dbutils within functions

# COMMAND ----------

# MAGIC %md ### Create a widget

# COMMAND ----------

@notebookFunction()
def create_input_widgets(dbutils: DBUtils):
  dbutils.widgets.text("widget_states", "AL", 'States') # Examples: CA, IL, IN,...
  dbutils.widgets.dropdown("widget_year", '2020', ['2018', '2019', '2020'], 'Example Dropdown Widget') # Examples: CA, IL, IN,...

# COMMAND ----------

# MAGIC %md ### Usage of widget variable

# COMMAND ----------

@dataFrameLoader(display=False)
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

# MAGIC %md ### Work with Pandas

# COMMAND ----------

from pandas.core.frame import DataFrame as pdDataFrame

# COMMAND ----------

@notebookFunction(add_year_value)
def spark_df_to_pandas(df: DataFrame) -> pdDataFrame:
    return df.toPandas()

# COMMAND ----------

type(spark_df_to_pandas.result)

# COMMAND ----------

@notebookFunction(spark_df_to_pandas.result)
def pandasTranformation(pd: pdDataFrame):
    pd2 = pd['WIDGET_YEAR']
    print(pd2)
    return pd2

# COMMAND ----------

# MAGIC %md ### Pass notebookFunction to other tranformations
# MAGIC See `other_python_function.result`

# COMMAND ----------

@notebookFunction()
def other_python_function(dbutils: DBUtils):
    return dbutils.widgets.get("widget_states")

# COMMAND ----------

@transformation(read_bronze_covid_tbl_template_2_confirmed_case, other_python_function.result, display=True)
def add_year_value_2(df: DataFrame, widget_test: str, logger: Logger):
    yearWidget = widget_test
    logger.info(f"Year Widget value: {yearWidget}")
    return (
        df
          .withColumn('other_python_function_VALUE', F.lit(yearWidget))
    )

# COMMAND ----------

# MAGIC %md ### Command to remove all Widget

# COMMAND ----------

#dbutils.widgets.removeAll()
