# pylint: disable = wrong-import-position
import os
from myproject.ContainerInit import initContainer

container = initContainer(os.environ['APP_ENV'])

from pyspark.sql.session import SparkSession
spark = container.get(SparkSession) # type: SparkSession

from loggerbundle.LoggerFactory import LoggerFactory
loggerFactory = container.get(LoggerFactory) # type: LoggerFactory

parameters = container.getParameters()
