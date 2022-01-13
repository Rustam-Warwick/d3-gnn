import abc
import copy
from abc import ABCMeta
from typing import TYPE_CHECKING, Tuple
from asyncio import Future, get_event_loop
from exceptions import OldVersionException
from elements import ReplicableGraphElement, ElementTypes, Rpc, GraphQuery, Op, ReplicaState, query_for_part

if TYPE_CHECKING:
    from elements import ReplicaState


class ElementFeature(ReplicableGraphElement, metaclass=ABCMeta):
    """ Base class for all the features of Edge,Vertex or more upcoming updates
        Most of its features are combing from the associated GraphElement, whereas an ElementFeature is also a GraphElement
        @todo make this generic maybe ? To infer weather it is replicable or not
    """
    def __init__(self, element: "GraphElement" = None, value: object = None, *args, **kwargs):
        self._value = value
        self.element: "GraphElement" = element
        super(ElementFeature, self).__init__(*args, **kwargs)

    def update_element(self, new_element: "ElementFeature") -> Tuple[bool, "GraphElement"]:
        """ No need to call the parent """
        memento = copy.copy(self)
        is_updated = not self._eq(self._value, new_element._value)
        if is_updated:
            self._value = new_element._value
            self.storage.update(self)
            self.storage.for_aggregator(lambda x: x.update_element_callback(self, memento))
        return is_updated, memento

    def sync_element(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        if self.state is ReplicaState.REPLICA: return super(ElementFeature, self).sync_element(new_element)
        return False, self

    @property
    def element_type(self):
        return ElementTypes.FEATURE

    @property
    def value(self):
        return self._value

    @property
    def master_part(self) -> int:
        if self.element:
            return self.element.master_part
        return self.master_part

    @property
    def replica_parts(self) -> list:
        if self.element:
            return self.element.replica_parts
        return list()

    @property
    def is_halo(self) -> bool:
        if self.element:
            return self.element.is_halo
        return self.is_halo

    def get_integer_clock(self):
        if self.element:
            return self.element.get_integer_clock()
        return super(ElementFeature, self).get_integer_clock()

    def set_integer_clock(self, value: int):
        if self.element:
            return self.element.set_integer_clock(value)
        super(ElementFeature, self).set_integer_clock(value)

    def del_integer_clock(self):
        if self.element:
            return self.element.del_integer_clock()
        super(ElementFeature, self).del_integer_clock()

    @abc.abstractmethod
    def _eq(self, old_value, new_value) -> bool:
        """ Given @param(values) of 2 Features if they are equal  """
        pass

    def cache_features(self):
        pass

    def __setitem__(self, key, value):
        pass

    def __getitem__(self, item):
        raise KeyError

    def __getstate__(self):
        if self.element:
            self.master = self.element.master
            self._clock = self.element._clock
            self._halo = self.element._halo

        state = super(ElementFeature, self).__getstate__()

        if "element" in state: del state['element']  # Do not serialize the element_feature
        return state
