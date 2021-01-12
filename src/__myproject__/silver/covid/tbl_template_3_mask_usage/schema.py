import pyspark.sql.types as T
def getSchema():
    return T.StructType([
        T.StructField('COUNTYFP', T.IntegerType(), True, {
            'source_column': '',
            'source_table': '',
            'comment': 'This is example comment',
        }),
        T.StructField('NEVER', T.DoubleType(), True, {
            'source_column': '',
            'source_table': '',
            'comment': 'Comment visible in Databricks',
        }),
        T.StructField('RARELY', T.DoubleType(), True, {
            'source_column': '',
            'source_table': '',
            'comment': '',
        }),
        T.StructField('SOMETIMES', T.DoubleType(), True, {
            'source_column': '',
            'source_table': '',
            'comment': '',
        }),
        T.StructField('FREQUENTLY', T.DoubleType(), True, {
            'source_column': '',
            'source_table': '',
            'comment': '',
        }),
        T.StructField('ALWAYS', T.DoubleType(), True, {
            'source_column': '',
            'source_table': '',
            'comment': '',
        }),
        T.StructField('EXECUTE_DATETIME', T.TimestampType(), False, {
            'source_column': '',
            'source_table': '',
            'comment': '',
        }),
        T.StructField('CONFIG_YAML_PARAMETER', T.StringType(), False, {
            'source_column': '',
            'source_table': '',
            'comment': '',
        })
])