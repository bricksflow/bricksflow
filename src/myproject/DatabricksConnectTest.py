import unittest
from databricksbundle.dbutils.DbUtilsWrapper import DbUtilsWrapper

class DatabricksConnectTest(unittest.TestCase):

    def test_basicInit(self):
        def createLazy():
            from pyspark.dbutils import DBUtils
            dbUtils = DBUtils()
            dbUtils.a = 5
            return dbUtils

        dbUtilsWrapper = DbUtilsWrapper(createLazy)
        result = dbUtilsWrapper.a

        self.assertEqual(5, result)

if __name__ == '__main__':
    unittest.main()
