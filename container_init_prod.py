#!/usr/bin/env python

"""
This script loads all services from the container to test if everything works properly

Differences from ContainerInitTest

* poetry dev dependencies must NOT be installed to simulate the Databricks PROD environment (however, databricks-connect python package is still needed)
* container is initialized in PROD environment compared to TEST environment used in ContainerInitTest
"""

from injecta.testing.servicesTester import testServices
from __myproject__.ContainerInit import initContainer

container = initContainer('prod')

testServices(container)
