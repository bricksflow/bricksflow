from typing import List
from databricksbundle.DatabricksBundle import DatabricksBundle
from pyfonybundles.Bundle import Bundle
from pyfony.kernel.BaseKernel import BaseKernel
from loggerbundle.LoggerBundle import LoggerBundle
from consolebundle.ConsoleBundle import ConsoleBundle
from databricksbundle.detector import isDatabricks

class Kernel(BaseKernel):

    def _registerBundles(self) -> List[Bundle]:
        bundles = []

        if isDatabricks() is False:
            from dbxdeploy.DbxDeployBundle import DbxDeployBundle # pylint: disable = import-outside-toplevel

            bundles += [
                DbxDeployBundle(),
            ]

        bundles += [
            ConsoleBundle(),
            LoggerBundle(),
            DatabricksBundle(),
        ]

        return bundles
