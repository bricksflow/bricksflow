import pyspark.sql.types as t


def get_schema():
    return t.StructType(
        [
            t.StructField("EXECUTE_DATE", t.DateType(), True),
            t.StructField("COUNTY_NAME", t.StringType(), True),
            t.StructField("AVG_NEVER", t.DoubleType(), True),
            t.StructField("AVG_RARELY", t.IntegerType(), True),
            t.StructField("AVG_SOMETIMES", t.IntegerType(), True),
            t.StructField("AVG_FREQUENTLY", t.IntegerType(), True),
            t.StructField("AVG_ALWAYS", t.IntegerType(), True),
        ]
    )
