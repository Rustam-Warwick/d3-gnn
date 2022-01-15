from elements.graph_element import GraphElement, ElementTypes
from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    from elements.vertex.base_vertex import BaseVertex
    from elements import Rpc, GraphElement


class BaseEdge(GraphElement):
    def __init__(self, src: 'BaseVertex', dest: 'BaseVertex', *args, **kwargs):
        super(BaseEdge, self).__init__(src.id + ":" + dest.id, *args, **kwargs)
        self.source: 'BaseVertex' = src
        self.destination: 'BaseVertex' = dest

    def create_element(self) -> bool:
        """ Vertices should be stored first, then Edges """
        self.source.create_element()
        self.destination.create_element()
        return super(BaseEdge, self).create_element()

    def update_element(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        src_old, src_changed = self.source.update_element(new_element.source)
        dest_old, dest_changed = self.destination.update_element(new_element.destination)
        is_changed, memento = super(BaseEdge, self).update_element(new_element)
        memento.source = src_old
        memento.destination = dest_old
        return is_changed, memento
        pass
    
    def attach_storage(self, storage: "GraphStorageProcess"):
        self.source.attach_storage(storage)
        self.destination.attach_storage(storage)
        super(BaseEdge, self).attach_storage(storage)

    def detach_storage(self):
        self.source.detach_storage()
        self.destination.detach_storage()
        super(BaseEdge, self).detach_storage()

    def __getstate__(self):
        state = super(BaseEdge, self).__getstate__()
        state['source'] = self.source
        state['destination'] = self.destination
        return state

    @property
    def element_type(self):
        return ElementTypes.EDGE
