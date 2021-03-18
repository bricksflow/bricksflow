import unittest
from injecta.testing.servicesTester import testServices
from pyfonycore.bootstrap import bootstrappedContainer


class ContainerTest(unittest.TestCase):
    def test_env_dev(self):
        container = bootstrappedContainer.init("dev")
        testServices(container)

    def test_env_prod(self):
        container = bootstrappedContainer.init("prod")
        testServices(container)


if __name__ == "__main__":
    unittest.main()
