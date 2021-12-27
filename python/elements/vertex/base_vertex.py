from elements.replicable_graph_element import ReplicableGraphElement
from elements.graph_element import ElementTypes
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from elements import Rpc, GraphElement

class BaseVertex(ReplicableGraphElement):
    def __init__(self, *args, **kwargs):
        super(BaseVertex, self).__init__(*args, **kwargs)


    def update(self, new_element: "GraphElement"):
        pass

    @property
    def element_type(self):
        return ElementTypes.VERTEX
