# pylint: disable = invalid-name, not-callable
from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.display import display as displayFunction
from databricksbundle.notebook.decorator.DecoratorMetaclass import DecoratorMetaclass
from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator

class feature(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(self, *args, description): # pylint: disable = unused-argument
        self._description = description

    def afterExecution(self, container: ContainerInterface):
        # entityManager.makeSureTablesExists

        entities = container.getParameters().featurestorebundle.entities

        displayFunction(self._result)
