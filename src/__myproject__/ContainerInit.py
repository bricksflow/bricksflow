from injecta.config.YamlConfigReader import YamlConfigReader
from injecta.container.ContainerInterface import ContainerInterface
from pyfonybundles.loader import pyfonyBundlesLoader
from pyfonycore.Kernel import Kernel
from __myproject__.libRoot import getLibRoot

def initContainer(appEnv: str) -> ContainerInterface:
    bundles = [*pyfonyBundlesLoader.loadBundles()]

    kernel = Kernel(
        appEnv,
        getLibRoot() + '/_config',
        YamlConfigReader(),
        bundles
    )

    container = kernel.initContainer()

    return container
