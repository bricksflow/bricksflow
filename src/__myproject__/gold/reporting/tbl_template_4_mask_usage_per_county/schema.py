import pyspark.sql.types as T
def getSchema():
    return T.StructType([
                T.StructField('EXECUTE_DATE', T.DateType(), True),
                T.StructField('COUNTY_NAME', T.StringType(), True),
                T.StructField('CONFIG_YAML_PARAMETER', T.StringType(), True),
                T.StructField('AVG_NEVER', T.DoubleType(), True),
                T.StructField('AVG_RARELY', T.IntegerType(), True),
                T.StructField('AVG_SOMETIMES', T.IntegerType(), True),
                T.StructField('AVG_FREQUENTLY', T.IntegerType(), True),
                T.StructField('AVG_ALWAYS', T.IntegerType(), True),
            ])
