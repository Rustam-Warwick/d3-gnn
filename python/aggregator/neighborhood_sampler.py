from aggregator import BaseAggregator
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from elements import GraphElement

class NeighborhoodSampler(BaseAggregator):
    def __init__(self, ident: str = "full_neighborhood_sampler", storage: "BaseStorage" = None):
        super().__init__(ident, storage)

    def run(self, element: "GraphElement", k: int):
        """ Samples full k hop neighborhood of element """
        pass

    def add_element_callback(self, element: "GraphElement"):
        pass

    def update_element_callback(self, element: "GraphElement"):
        pass

    def sync_element_callback(self, element: "GraphElement"):
        pass

    def rpc_element_callback(self, element: "GraphElement"):
        pass
