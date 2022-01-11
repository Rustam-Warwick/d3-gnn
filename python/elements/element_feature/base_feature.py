import abc
import copy
from abc import ABCMeta
from typing import TYPE_CHECKING, Tuple
from asyncio import Future, get_event_loop
from exceptions import OldVersionException
from elements import GraphElement, ElementTypes, Rpc, GraphQuery, Op, ReplicaState, query_for_part

if TYPE_CHECKING:
    from elements import ReplicaState


class ElementFeature(GraphElement, metaclass=ABCMeta):
    """ Base class for all the features of Edge,Vertex or more upcoming updates
        Most of its features are combing from the associated GraphElement, whereas an ElementFeature is also a GraphElement

    """

    def __init__(self, element: "GraphElement" = None, value: object = None, *args, **kwargs):
        self._value = value
        self.element: "GraphElement" = element
        super(ElementFeature, self).__init__(*args, **kwargs)

    def update(self, new_element: "ElementFeature") -> bool:
        """ Swapping the 2 states for memory efficiency """
        memento = copy.copy(self)
        is_updated = not self._eq(self._value, new_element._value)
        if is_updated:
            self._value = new_element._value
            self.storage.update(self)
            self.storage.for_aggregator(lambda x: x.update_element_callback(self, memento))
        return is_updated

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

    @property
    def is_initialized(self) -> bool:
        return self.element.is_initialized

    @property
    def is_waiting(self) -> bool:
        return self.element.is_waiting

    @property
    def is_halo(self) -> bool:
        return self.element.is_halo

    def get_integer_clock(self):
        return self.element.get_integer_clock()

    def set_integer_clock(self, value: int):
        self.element.set_integer_clock(value)

    def del_integer_clock(self):
        self.element.del_integer_clock()

    @abc.abstractmethod
    def _eq(self, old_value, new_value) -> bool:
        """ Given @param(values) of 2 Features if they are equal  """
        pass

    def __getstate__(self):
        state = super(ElementFeature, self).__getstate__()
        if "element" in state: del state['element']  # Do not serialize the element_feature
        return state
