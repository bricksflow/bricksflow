from injecta.config.YamlConfigReader import YamlConfigReader
from injecta.container.ContainerInterface import ContainerInterface
from myproject.Kernel import Kernel
from myproject.libRoot import getLibRoot

def initContainer(appEnv: str) -> ContainerInterface:
    kernel = Kernel(
        appEnv,
        getLibRoot() + '/_config',
        YamlConfigReader()
    )

    return kernel.initContainer()
