import unittest
from injecta.testing.servicesTester import testServices
from pyfonycore.bootstrap import bootstrappedContainer

class AppTest(unittest.TestCase):

    def test_env_dev(self):
        testServices(bootstrappedContainer.init('dev'))

    def test_env_prod(self):
        testServices(bootstrappedContainer.init('prod'))

if __name__ == '__main__':
    unittest.main()
