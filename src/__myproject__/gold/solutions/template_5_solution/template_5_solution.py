# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #5: Pandas

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import data_frame_loader, notebook_function
from datalakebundle.table.TableManager import TableManager

# COMMAND ----------


@data_frame_loader(display=False)
def read_bronze_covid_tbl_template_2_confirmed_case(spark: SparkSession, logger: Logger, table_manager: TableManager):
    return spark.read.table(table_manager.get_name("bronze_covid.tbl_template_2_confirmed_cases")).select(
        "countyFIPS", "County_Name", "State", "stateFIPS"
    )


# COMMAND ----------

# MAGIC %md #### Working with Pandas

# COMMAND ----------

from pandas.core.frame import DataFrame as pdDataFrame  # noqa: E402

# COMMAND ----------


@notebook_function(read_bronze_covid_tbl_template_2_confirmed_case)
def spark_df_to_pandas(df: DataFrame) -> pdDataFrame:
    return df.toPandas()


# COMMAND ----------

type(spark_df_to_pandas.result)

# COMMAND ----------


@notebook_function(spark_df_to_pandas)
def pandas_tranformation(pd: pdDataFrame):
    pd2 = pd["County_Name"]
    print(pd2)
    return pd2
