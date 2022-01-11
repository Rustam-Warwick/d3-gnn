from elements.replicable_graph_element import ReplicableGraphElement
from elements.graph_element import ElementTypes
from typing import TYPE_CHECKING, Tuple
from exceptions import OldVersionException
from dgl.distributed import DistGraph, sample_neighbors

if TYPE_CHECKING:
    from elements import Rpc, GraphElement
    from elements.element_feature import ElementFeature


class BaseVertex(ReplicableGraphElement):
    def __init__(self, *args, **kwargs):
        super(BaseVertex, self).__init__(*args, **kwargs)

    @property
    def element_type(self):
        return ElementTypes.VERTEX
