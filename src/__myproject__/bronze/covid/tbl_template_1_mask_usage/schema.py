import pyspark.sql.types as T
def getSchema():
    return T.StructType([
        T.StructField('COUNTYFP', T.IntegerType(), True),
        T.StructField('NEVER', T.DoubleType(), True),
        T.StructField('RARELY', T.DoubleType(), True),
        T.StructField('SOMETIMES', T.DoubleType(), True),
        T.StructField('FREQUENTLY', T.DoubleType(), True),
        T.StructField('ALWAYS', T.DoubleType(), True),
        T.StructField('INSERT_TS', T.TimestampType(), True),
    ])