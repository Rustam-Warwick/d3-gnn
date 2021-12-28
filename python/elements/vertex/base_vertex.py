from elements.replicable_graph_element import ReplicableGraphElement
from elements.graph_element import ElementTypes
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elements import Rpc, GraphElement
    from elements.feature import Feature


class BaseVertex(ReplicableGraphElement):
    def __init__(self, *args, **kwargs):
        super(BaseVertex, self).__init__(*args, **kwargs)

    def update(self, new_element: "BaseVertex"):
        for i in self.features:
            feature:"Feature" = i[1]
            attr = getattr(new_element, i[0], None)
            if not attr:
                # New element does not contain so remove here as well
                delattr(self,i[0])
            else:
                feature.update(attr)

    @property
    def element_type(self):
        return ElementTypes.VERTEX
