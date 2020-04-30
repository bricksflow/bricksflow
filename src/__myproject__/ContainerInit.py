from injecta.config.YamlConfigReader import YamlConfigReader
from injecta.container.ContainerInterface import ContainerInterface
from __myproject__.Kernel import Kernel
from __myproject__.libRoot import getLibRoot

def initContainer(appEnv: str) -> ContainerInterface:
    kernel = Kernel(
        appEnv,
        getLibRoot() + '/_config',
        YamlConfigReader()
    )

    return kernel.initContainer()
