from elements.graph_element import GraphElement,ElementTypes
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from elements.vertex.base_vertex import BaseVertex


class BaseEdge(GraphElement):
    def __init__(self, src: 'BaseVertex', dest: 'BaseVertex', *args, **kwargs):
        super(BaseEdge, self).__init__(src.id+dest.id, *args, **kwargs)
        self.source:'BaseVertex' = src
        self.destination:'BaseVertex' = dest

    @property
    def element_type(self):
        return ElementTypes.EDGE


