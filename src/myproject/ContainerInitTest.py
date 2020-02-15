import os
import unittest
from injecta.testing.servicesTester import testServices
from myproject.ContainerInit import initContainer

class ContainerInitTest(unittest.TestCase):

    def test_init(self):
        appEnv = os.environ['APP_ENV']
        container = initContainer(appEnv)

        testServices(container)

if __name__ == '__main__':
    unittest.main()
