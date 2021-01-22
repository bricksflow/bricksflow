# Databricks notebook source
# this notebook should be included (% run) by all other Databricks notebooks to load the Master Package properly

# COMMAND ----------

# Bricksflow's environment setup
# It is recommended to set the APP_ENV variable on cluster-level rather than in a notebook
import os

if 'APP_ENV' not in os.environ:
    os.environ['APP_ENV'] = 'dev'

# COMMAND ----------

# MAGIC %installMasterPackageWhl