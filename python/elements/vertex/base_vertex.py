from elements.replicable_graph_element import ReplicableGraphElement
from elements.graph_element import ElementTypes
from typing import TYPE_CHECKING,Tuple
from exceptions import OldVersionException

if TYPE_CHECKING:
    from elements import Rpc, GraphElement
    from elements.feature import Feature


class BaseVertex(ReplicableGraphElement):
    def __init__(self, *args, **kwargs):
        super(BaseVertex, self).__init__(*args, **kwargs)

    def update(self, new_element: "BaseVertex") -> Tuple[bool, "GraphElement"]:
        return False, self

    def _sync(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        is_changed, res = super()._sync(new_element)
        for name, feature in self.features:
            attr = getattr(new_element, name, None)
            if not attr:
                delattr(self, name)
            else:
                is_changed |= self.storage._sync(feature, attr)
        return is_changed, self

    def _update(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        is_changed, res = super()._update(new_element)
        for name, feature in self.features:
            attr = getattr(new_element, name, None)
            if not attr:
                delattr(self, name)
                # @todo Delattr should delete feature right?
            else:
                is_changed |= self.storage._update(feature, attr)
        return is_changed, self

    @property
    def element_type(self):
        return ElementTypes.VERTEX
