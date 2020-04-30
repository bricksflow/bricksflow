# Databricks notebook source
# MAGIC %run ../app/install_master_package

# COMMAND ----------

from __myproject__.app.init import spark, loggerFactory, parameters
from time import time, sleep

logger = loggerFactory.create('my_test_logger')

# COMMAND ----------

logger.info('Starting', extra={
    'time': time(),
})

sleep(1)

logger.warning(parameters.myparameter.myvalue)

a = [1, 2, 3, 4]
b = [2, 3, 4, 8]
df = spark.createDataFrame([a, b], schema=['a', 'b'])

df.show()

logger.info('Finished', extra={
    'time': time(),
})
