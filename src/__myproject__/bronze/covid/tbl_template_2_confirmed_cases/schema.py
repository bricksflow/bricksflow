import pyspark.sql.types as t


def get_schema():  # noqa: N802
    return t.StructType(
        [
            t.StructField("countyFIPS", t.IntegerType(), True),
            t.StructField("County_Name", t.StringType(), True),
            t.StructField("State", t.StringType(), True),
            t.StructField("stateFIPS", t.IntegerType(), True),
            t.StructField("1/22/20", t.IntegerType(), True),
            t.StructField("1/23/20", t.IntegerType(), True),
            t.StructField("1/24/20", t.IntegerType(), True),
            t.StructField("1/25/20", t.IntegerType(), True),
            t.StructField("1/26/20", t.IntegerType(), True),
            t.StructField("1/27/20", t.IntegerType(), True),
            t.StructField("1/28/20", t.IntegerType(), True),
            t.StructField("1/29/20", t.IntegerType(), True),
            t.StructField("1/30/20", t.IntegerType(), True),
            t.StructField("1/31/20", t.IntegerType(), True),
            t.StructField("2/1/20", t.IntegerType(), True),
            t.StructField("2/2/20", t.IntegerType(), True),
            t.StructField("2/3/20", t.IntegerType(), True),
            t.StructField("2/4/20", t.IntegerType(), True),
            t.StructField("2/5/20", t.IntegerType(), True),
            t.StructField("2/6/20", t.IntegerType(), True),
            t.StructField("2/7/20", t.IntegerType(), True),
            t.StructField("2/8/20", t.IntegerType(), True),
            t.StructField("2/9/20", t.IntegerType(), True),
            t.StructField("2/10/20", t.IntegerType(), True),
            t.StructField("2/11/20", t.IntegerType(), True),
            t.StructField("2/12/20", t.IntegerType(), True),
            t.StructField("2/13/20", t.IntegerType(), True),
            t.StructField("2/14/20", t.IntegerType(), True),
            t.StructField("2/15/20", t.IntegerType(), True),
            t.StructField("2/16/20", t.IntegerType(), True),
            t.StructField("2/17/20", t.IntegerType(), True),
            t.StructField("2/18/20", t.IntegerType(), True),
            t.StructField("2/19/20", t.IntegerType(), True),
            t.StructField("2/20/20", t.IntegerType(), True),
            t.StructField("2/21/20", t.IntegerType(), True),
            t.StructField("2/22/20", t.IntegerType(), True),
            t.StructField("2/23/20", t.IntegerType(), True),
            t.StructField("2/24/20", t.IntegerType(), True),
            t.StructField("2/25/20", t.IntegerType(), True),
            t.StructField("2/26/20", t.IntegerType(), True),
            t.StructField("2/27/20", t.IntegerType(), True),
            t.StructField("2/28/20", t.IntegerType(), True),
            t.StructField("2/29/20", t.IntegerType(), True),
            t.StructField("3/1/20", t.IntegerType(), True),
            t.StructField("3/2/20", t.IntegerType(), True),
            t.StructField("3/3/20", t.IntegerType(), True),
            t.StructField("3/4/20", t.IntegerType(), True),
            t.StructField("3/5/20", t.IntegerType(), True),
            t.StructField("3/6/20", t.IntegerType(), True),
            t.StructField("3/7/20", t.IntegerType(), True),
            t.StructField("3/8/20", t.IntegerType(), True),
            t.StructField("3/9/20", t.IntegerType(), True),
            t.StructField("3/10/20", t.IntegerType(), True),
            t.StructField("3/11/20", t.IntegerType(), True),
            t.StructField("3/12/20", t.IntegerType(), True),
            t.StructField("3/13/20", t.IntegerType(), True),
            t.StructField("3/14/20", t.IntegerType(), True),
            t.StructField("3/15/20", t.IntegerType(), True),
            t.StructField("3/16/20", t.IntegerType(), True),
            t.StructField("3/17/20", t.IntegerType(), True),
            t.StructField("3/18/20", t.IntegerType(), True),
            t.StructField("3/19/20", t.IntegerType(), True),
            t.StructField("3/20/20", t.IntegerType(), True),
            t.StructField("3/21/20", t.IntegerType(), True),
            t.StructField("3/22/20", t.IntegerType(), True),
            t.StructField("3/23/20", t.IntegerType(), True),
            t.StructField("3/24/20", t.IntegerType(), True),
            t.StructField("3/25/20", t.IntegerType(), True),
            t.StructField("3/26/20", t.IntegerType(), True),
            t.StructField("3/27/20", t.IntegerType(), True),
            t.StructField("3/28/20", t.IntegerType(), True),
            t.StructField("3/29/20", t.IntegerType(), True),
            t.StructField("3/30/20", t.IntegerType(), True),
            t.StructField("3/31/20", t.IntegerType(), True),
            t.StructField("4/1/20", t.IntegerType(), True),
            t.StructField("4/2/20", t.IntegerType(), True),
            t.StructField("4/3/20", t.IntegerType(), True),
            t.StructField("4/4/20", t.IntegerType(), True),
            t.StructField("4/5/20", t.IntegerType(), True),
            t.StructField("4/6/20", t.IntegerType(), True),
            t.StructField("4/7/20", t.IntegerType(), True),
            t.StructField("4/8/20", t.IntegerType(), True),
            t.StructField("4/9/20", t.IntegerType(), True),
            t.StructField("4/10/20", t.IntegerType(), True),
            t.StructField("4/11/20", t.IntegerType(), True),
            t.StructField("4/12/20", t.IntegerType(), True),
            t.StructField("4/13/20", t.IntegerType(), True),
            t.StructField("4/14/20", t.IntegerType(), True),
            t.StructField("4/15/20", t.IntegerType(), True),
            t.StructField("4/16/20", t.IntegerType(), True),
            t.StructField("4/17/20", t.IntegerType(), True),
            t.StructField("4/18/20", t.IntegerType(), True),
            t.StructField("4/19/20", t.IntegerType(), True),
            t.StructField("4/20/20", t.IntegerType(), True),
            t.StructField("4/21/20", t.IntegerType(), True),
            t.StructField("4/22/20", t.IntegerType(), True),
            t.StructField("4/23/20", t.IntegerType(), True),
            t.StructField("4/24/20", t.IntegerType(), True),
            t.StructField("4/25/20", t.IntegerType(), True),
            t.StructField("4/26/20", t.IntegerType(), True),
            t.StructField("4/27/20", t.IntegerType(), True),
            t.StructField("4/28/20", t.IntegerType(), True),
            t.StructField("4/29/20", t.IntegerType(), True),
            t.StructField("4/30/20", t.IntegerType(), True),
            t.StructField("5/1/20", t.IntegerType(), True),
            t.StructField("5/2/20", t.IntegerType(), True),
            t.StructField("5/3/20", t.IntegerType(), True),
            t.StructField("5/4/20", t.IntegerType(), True),
            t.StructField("5/5/20", t.IntegerType(), True),
            t.StructField("5/6/20", t.IntegerType(), True),
            t.StructField("5/7/20", t.IntegerType(), True),
            t.StructField("5/8/20", t.IntegerType(), True),
            t.StructField("5/9/20", t.IntegerType(), True),
            t.StructField("5/10/20", t.IntegerType(), True),
            t.StructField("5/11/20", t.IntegerType(), True),
            t.StructField("5/12/20", t.IntegerType(), True),
            t.StructField("5/13/20", t.IntegerType(), True),
            t.StructField("5/14/20", t.IntegerType(), True),
            t.StructField("5/15/20", t.IntegerType(), True),
            t.StructField("5/16/20", t.IntegerType(), True),
            t.StructField("5/17/20", t.IntegerType(), True),
            t.StructField("5/18/20", t.IntegerType(), True),
            t.StructField("5/19/20", t.IntegerType(), True),
            t.StructField("5/20/20", t.IntegerType(), True),
            t.StructField("5/21/20", t.IntegerType(), True),
            t.StructField("5/22/20", t.IntegerType(), True),
            t.StructField("5/23/20", t.IntegerType(), True),
            t.StructField("5/24/20", t.IntegerType(), True),
            t.StructField("5/25/20", t.IntegerType(), True),
            t.StructField("5/26/20", t.IntegerType(), True),
            t.StructField("5/27/20", t.IntegerType(), True),
            t.StructField("5/28/20", t.IntegerType(), True),
            t.StructField("5/29/20", t.IntegerType(), True),
            t.StructField("5/30/20", t.IntegerType(), True),
            t.StructField("5/31/20", t.IntegerType(), True),
            t.StructField("6/1/20", t.IntegerType(), True),
            t.StructField("6/2/20", t.IntegerType(), True),
            t.StructField("6/3/20", t.IntegerType(), True),
            t.StructField("6/4/20", t.IntegerType(), True),
            t.StructField("6/5/20", t.IntegerType(), True),
            t.StructField("6/6/20", t.IntegerType(), True),
            t.StructField("6/7/20", t.IntegerType(), True),
            t.StructField("6/8/20", t.IntegerType(), True),
            t.StructField("6/9/20", t.IntegerType(), True),
            t.StructField("6/10/20", t.IntegerType(), True),
            t.StructField("6/11/20", t.IntegerType(), True),
            t.StructField("6/12/20", t.IntegerType(), True),
            t.StructField("6/13/20", t.IntegerType(), True),
            t.StructField("6/14/20", t.IntegerType(), True),
            t.StructField("6/15/20", t.IntegerType(), True),
            t.StructField("6/16/20", t.IntegerType(), True),
            t.StructField("6/17/20", t.IntegerType(), True),
            t.StructField("6/18/20", t.IntegerType(), True),
            t.StructField("6/19/20", t.IntegerType(), True),
            t.StructField("6/20/20", t.IntegerType(), True),
            t.StructField("6/21/20", t.IntegerType(), True),
            t.StructField("6/22/20", t.IntegerType(), True),
            t.StructField("6/23/20", t.IntegerType(), True),
            t.StructField("6/24/20", t.IntegerType(), True),
            t.StructField("6/25/20", t.IntegerType(), True),
            t.StructField("6/26/20", t.IntegerType(), True),
            t.StructField("6/27/20", t.IntegerType(), True),
            t.StructField("6/28/20", t.IntegerType(), True),
            t.StructField("6/29/20", t.IntegerType(), True),
            t.StructField("6/30/20", t.IntegerType(), True),
            t.StructField("7/1/20", t.IntegerType(), True),
            t.StructField("7/2/20", t.IntegerType(), True),
            t.StructField("7/3/20", t.IntegerType(), True),
            t.StructField("7/4/20", t.IntegerType(), True),
            t.StructField("7/5/20", t.IntegerType(), True),
            t.StructField("7/6/20", t.IntegerType(), True),
            t.StructField("7/7/20", t.IntegerType(), True),
            t.StructField("7/8/20", t.IntegerType(), True),
            t.StructField("7/9/20", t.IntegerType(), True),
            t.StructField("7/10/20", t.IntegerType(), True),
            t.StructField("7/11/20", t.IntegerType(), True),
            t.StructField("7/12/20", t.IntegerType(), True),
            t.StructField("7/13/20", t.IntegerType(), True),
            t.StructField("7/14/20", t.IntegerType(), True),
            t.StructField("7/15/20", t.IntegerType(), True),
            t.StructField("7/16/20", t.IntegerType(), True),
            t.StructField("7/17/20", t.IntegerType(), True),
            t.StructField("7/18/20", t.IntegerType(), True),
            t.StructField("7/19/20", t.IntegerType(), True),
            t.StructField("7/20/20", t.IntegerType(), True),
            t.StructField("7/21/20", t.IntegerType(), True),
            t.StructField("7/22/20", t.IntegerType(), True),
            t.StructField("7/23/20", t.IntegerType(), True),
            t.StructField("7/24/20", t.IntegerType(), True),
            t.StructField("7/25/20", t.IntegerType(), True),
            t.StructField("7/26/20", t.IntegerType(), True),
            t.StructField("7/27/20", t.IntegerType(), True),
            t.StructField("7/28/20", t.IntegerType(), True),
            t.StructField("7/29/20", t.IntegerType(), True),
            t.StructField("7/30/20", t.IntegerType(), True),
            t.StructField("7/31/20", t.IntegerType(), True),
            t.StructField("8/1/20", t.IntegerType(), True),
            t.StructField("8/2/20", t.IntegerType(), True),
            t.StructField("8/3/20", t.IntegerType(), True),
            t.StructField("8/4/20", t.IntegerType(), True),
            t.StructField("8/5/20", t.IntegerType(), True),
            t.StructField("8/6/20", t.IntegerType(), True),
            t.StructField("8/7/20", t.IntegerType(), True),
            t.StructField("8/8/20", t.IntegerType(), True),
            t.StructField("8/9/20", t.IntegerType(), True),
            t.StructField("8/10/20", t.IntegerType(), True),
            t.StructField("8/11/20", t.IntegerType(), True),
            t.StructField("8/12/20", t.IntegerType(), True),
            t.StructField("8/13/20", t.IntegerType(), True),
            t.StructField("8/14/20", t.IntegerType(), True),
            t.StructField("8/15/20", t.IntegerType(), True),
            t.StructField("8/16/20", t.IntegerType(), True),
            t.StructField("8/17/20", t.IntegerType(), True),
            t.StructField("8/18/20", t.IntegerType(), True),
            t.StructField("8/19/20", t.IntegerType(), True),
            t.StructField("8/20/20", t.IntegerType(), True),
            t.StructField("8/21/20", t.IntegerType(), True),
            t.StructField("8/22/20", t.IntegerType(), True),
            t.StructField("8/23/20", t.IntegerType(), True),
            t.StructField("8/24/20", t.IntegerType(), True),
            t.StructField("8/25/20", t.IntegerType(), True),
            t.StructField("8/26/20", t.IntegerType(), True),
            t.StructField("8/27/20", t.IntegerType(), True),
            t.StructField("8/28/20", t.IntegerType(), True),
            t.StructField("8/29/20", t.IntegerType(), True),
            t.StructField("8/30/20", t.IntegerType(), True),
            t.StructField("8/31/20", t.IntegerType(), True),
            t.StructField("9/1/20", t.IntegerType(), True),
            t.StructField("9/2/20", t.IntegerType(), True),
            t.StructField("9/3/20", t.IntegerType(), True),
            t.StructField("9/4/20", t.IntegerType(), True),
            t.StructField("9/5/20", t.IntegerType(), True),
            t.StructField("9/6/20", t.IntegerType(), True),
            t.StructField("9/7/20", t.IntegerType(), True),
            t.StructField("9/8/20", t.IntegerType(), True),
            t.StructField("9/9/20", t.IntegerType(), True),
            t.StructField("9/10/20", t.IntegerType(), True),
            t.StructField("9/11/20", t.IntegerType(), True),
            t.StructField("9/12/20", t.IntegerType(), True),
        ]
    )
