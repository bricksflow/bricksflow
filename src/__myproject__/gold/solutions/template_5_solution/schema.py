import pyspark.sql.types as t


def getSchema():  # noqa: N802
    return t.StructType(
        [
            t.StructField("EXECUTE_DATE", t.DateType(), True),
            t.StructField("COUNTY_NAME", t.StringType(), True),
            t.StructField("CONFIG_YAML_PARAMETER", t.StringType(), True),
            t.StructField("AVG_NEVER", t.DoubleType(), True),
            t.StructField("AVG_RARELY", t.IntegerType(), True),
            t.StructField("AVG_SOMETIMES", t.IntegerType(), True),
            t.StructField("AVG_FREQUENTLY", t.IntegerType(), True),
            t.StructField("AVG_ALWAYS", t.IntegerType(), True),
        ]
    )
