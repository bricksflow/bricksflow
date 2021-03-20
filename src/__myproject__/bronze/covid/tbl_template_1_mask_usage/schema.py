import pyspark.sql.types as t


def get_schema():
    return t.StructType(
        [
            t.StructField("COUNTYFP", t.IntegerType(), True),
            t.StructField("NEVER", t.DoubleType(), True),
            t.StructField("RARELY", t.DoubleType(), True),
            t.StructField("SOMETIMES", t.DoubleType(), True),
            t.StructField("FREQUENTLY", t.DoubleType(), True),
            t.StructField("ALWAYS", t.DoubleType(), True),
            t.StructField("INSERT_TS", t.TimestampType(), True),
        ]
    )
