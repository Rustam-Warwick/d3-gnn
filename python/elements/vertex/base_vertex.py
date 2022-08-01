from elements.replicable_graph_element import ReplicableGraphElement
from elements.graph_element import ElementTypes


class BaseVertex(ReplicableGraphElement):
    def __init__(self, element_id: str = None, *args, **kwargs):
        new_element_id = "%s%s" % (ElementTypes.VERTEX.value, element_id)
        super(BaseVertex, self).__init__(element_id=new_element_id, *args, **kwargs)

    @property
    def element_type(self):
        return ElementTypes.VERTEX
