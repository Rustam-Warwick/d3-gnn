import abc
from abc import ABCMeta
from typing import TYPE_CHECKING, Tuple
from asyncio import Future, get_event_loop
from exceptions import OldVersionException
from elements import GraphElement, ElementTypes, Rpc, GraphQuery, Op, ReplicaState, query_for_part

if TYPE_CHECKING:
    from elements import ReplicaState


class Feature(GraphElement, metaclass=ABCMeta):
    """ Base class for all the features of Edge,Vertex or more
        Implemented in a way that Features can exist without being attached to element al long as they have unique ids
    """

    def __init__(self, field_name: str, element: "GraphElement", value: object = None, *args, **kwargs):
        feature_id = "%s:%s:%s" % (element.element_type.value, element.id, field_name)
        self.field_name = field_name
        self._value = value
        self.element: "GraphElement" = element
        super(Feature, self).__init__(element_id=feature_id, *args, **kwargs)

    def update(self, new_element: "Feature") -> Tuple[bool, "GraphElement"]:
        """ Swapping the 2 states for memory efficiency """
        res = self._eq(self._value, new_element._value)
        memento = self._value
        self._value = new_element._value
        new_element._value = memento
        return not res, new_element

    @property
    def element_type(self):
        return ElementTypes.FEATURE

    @property
    def value(self):
        return self._value

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

    @abc.abstractmethod
    def _eq(self, old_value, new_value) -> bool:
        """ Given @param(values) of 2 Features if they are equal  """
        pass

    def __getstate__(self):
        state = super(Feature, self).__getstate__()
        if "element" in state: del state['element']  # Do not serialize the feature
        return state
