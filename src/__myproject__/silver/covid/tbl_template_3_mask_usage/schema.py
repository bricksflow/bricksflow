import pyspark.sql.types as t


def getSchema():  # noqa: N802
    return t.StructType(
        [
            t.StructField(
                "COUNTYFP",
                t.IntegerType(),
                True,
                {
                    "source_column": "",
                    "source_table": "",
                    "comment": "This is example comment",
                },
            ),
            t.StructField(
                "NEVER",
                t.DoubleType(),
                True,
                {
                    "source_column": "",
                    "source_table": "",
                    "comment": "Comment visible in Databricks",
                },
            ),
            t.StructField(
                "RARELY",
                t.DoubleType(),
                True,
                {
                    "source_column": "",
                    "source_table": "",
                    "comment": "",
                },
            ),
            t.StructField(
                "SOMETIMES",
                t.DoubleType(),
                True,
                {
                    "source_column": "",
                    "source_table": "",
                    "comment": "",
                },
            ),
            t.StructField(
                "FREQUENTLY",
                t.DoubleType(),
                True,
                {
                    "source_column": "",
                    "source_table": "",
                    "comment": "",
                },
            ),
            t.StructField(
                "ALWAYS",
                t.DoubleType(),
                True,
                {
                    "source_column": "",
                    "source_table": "",
                    "comment": "",
                },
            ),
            t.StructField(
                "EXECUTE_DATETIME",
                t.TimestampType(),
                False,
                {
                    "source_column": "",
                    "source_table": "",
                    "comment": "",
                },
            ),
        ]
    )
