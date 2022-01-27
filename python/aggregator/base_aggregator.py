import abc
from abc import ABCMeta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage.gnn_layer import GNNLayerProcess
    from elements import GraphElement


class BaseAggregator(metaclass=ABCMeta):
    """ Base class for all the Aggregators in the storage """

    def __init__(self, ident: str, storage: "GNNLayerProcess" = None):
        self.storage: "GNNLayerProcess" = storage  # Storage attached to
        self.id: str = ident  # Unique name per storage of this aggregator, accessed through storage.aggregators[str]

    @abc.abstractmethod
    def open(self, *args, **kwargs):
        """ When the Storage Task starts in task-manager """
        pass

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        """ Called with query in *args, when there is associated AGG event coming in with corresponding aggregator
        name """
        pass

    @abc.abstractmethod
    def add_element_callback(self, element: "GraphElement"):
        """ Callback after Graph Element is added """
        pass

    @abc.abstractmethod
    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        """ When a GraphElement is updated, SYNC, UPDATE, RPC  """
        pass
