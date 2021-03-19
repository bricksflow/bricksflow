import unittest
from injecta.testing.services_tester import test_services
from pyfonycore.bootstrap import bootstrapped_container


class ContainerTest(unittest.TestCase):
    def test_env_dev(self):
        container = bootstrapped_container.init("dev")
        test_services(container)

    def test_env_prod(self):
        container = bootstrapped_container.init("prod")
        test_services(container)


if __name__ == "__main__":
    unittest.main()
