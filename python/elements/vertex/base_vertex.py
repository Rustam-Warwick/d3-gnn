from elements.replicable_graph_element import ReplicableGraphElement
from elements.graph_element import ElementTypes


class BaseVertex(ReplicableGraphElement):
    def __init__(self, *args, **kwargs):
        super(BaseVertex, self).__init__(*args, **kwargs)

    @property
    def element_type(self):
        return ElementTypes.VERTEX
