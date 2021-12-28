import abc
from abc import ABCMeta
from typing import TYPE_CHECKING

import elements.graph_element
from elements import GraphElement, ElementTypes, Rpc, GraphQuery, Op, ReplicaState, query_for_part

if TYPE_CHECKING:
    from elements import ReplicaState


class Feature(GraphElement, metaclass=ABCMeta):
    """ Base class for all the features of Edge,Vertex or more
        Implemented in a way that Features can exist without being attached to element al long as they have unique ids
    """

    def __init__(self, field_name: str, element: "GraphElement", value: object = None, *args, **kwargs):
        feature_id = "%s:%s:%s" % (element.element_type.value, element.id, field_name)
        self.value = value
        self.element: "GraphElement" = element
        super(Feature, self).__init__(element_id=feature_id, *args, **kwargs)

    def update(self, new_element: "Feature") -> bool:
        self.value = new_element.value
        return True

    @property
    def element_type(self):
        return ElementTypes.FEATURE

    @property
    def state(self) -> ReplicaState:
        return self.element.state

    @property
    def is_replicable(self) -> bool:
        return self.element.is_replicable

    @property
    def master_part(self) -> int:
        return self.element.master_part

    @property
    def replica_parts(self) -> list:
        return self.element.replica_parts

    def __getstate__(self):
        state = super(Feature, self).__getstate__()
        if "element" in state: del state['element']  # Do not serialize the feature
        return state
