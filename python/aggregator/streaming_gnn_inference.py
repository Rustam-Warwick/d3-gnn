from aggregator import BaseAggregator
from elements import ElementTypes
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from elements.edge import SimpleEdge
    from elements.vertex import SimpleVertex


class StreamingGNNInference(BaseAggregator):

    def __init__(self, ident: str = "streaming_gnn", storage: "BaseStorage" = None):
        super().__init__(ident, storage)

    def run(self, *args, **kwargs):
        pass

    def add_element_callback(self, element: "GraphElement"):
        if element.element_type is ElementTypes.EDGE:
            edge: "SimpleEdge" = element
            src: "SimpleVertex" = edge.source
            dest: "SimpleVertex" = edge.destination
            src.agg += src.image
        pass

    def update_element_callback(self, element: "GraphElement"):
        pass

    def sync_element_callback(self, element: "GraphElement"):
        pass

    def rpc_element_callback(self, element: "GraphElement"):
        pass
