import abc
import copy
from abc import ABCMeta
from typing import TYPE_CHECKING, Tuple
import re
from elements import ReplicableGraphElement, ElementTypes, ReplicaState

if TYPE_CHECKING:
    from elements import  GraphElement


class ReplicableFeature(ReplicableGraphElement, metaclass=ABCMeta):
    """ Base class for all the features of Edge,Vertex or more upcoming updates
        Most of its features are combing from the associated GraphElement, whereas an ElementFeature is also a GraphElement
        @todo make this generic maybe ? To infer weather it is replicable or not: Hard to do without overheads
    """
    copy_fields = ("_value",)  # Only deep copy _value if needed rest is just reference attachment

    def __init__(self, element: "GraphElement" = None, value: object = None, *args, **kwargs):
        self._value = value
        self.element: "GraphElement" = element
        super(ReplicableFeature, self).__init__(*args, **kwargs)

    def update_element(self, new_element: "ReplicableFeature") -> Tuple[bool, "GraphElement"]:
        """ Update this feature """
        memento = copy.copy(self)
        memento.element = None
        is_updated = not self._value_eq_(self._value, new_element._value)
        if is_updated:
            self._value = new_element._value
            if self.state is ReplicaState.MASTER: self.integer_clock += 1
            self.storage.update(self)
            self.storage.update(self.element)
            self.storage.for_aggregator(lambda x: x.update_element_callback(self, memento))
        return is_updated, memento

    def sync_element(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        if self.state is ReplicaState.REPLICA: return super(ReplicableFeature, self).sync_element(new_element)
        return False, self

    @abc.abstractmethod
    def _value_eq_(self, old_value, new_value) -> bool:
        """ Given @param(values) of 2 Features if they are equal  """
        pass

    @property
    def field_name(self):
        """ Retrieve field name from the id """
        group = re.search("[\w:]+:(?P<feature_name>\w+)", self.id)
        if group:
            return group['feature_name']
        raise KeyError

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
        return super(ReplicableFeature, self).get_integer_clock()

    def set_integer_clock(self, value: int):
        if self.element:
            return self.element.set_integer_clock(value)
        super(ReplicableFeature, self).set_integer_clock(value)

    def del_integer_clock(self):
        if self.element:
            return self.element.del_integer_clock()
        super(ReplicableFeature, self).del_integer_clock()

    integer_clock = property(get_integer_clock, set_integer_clock, del_integer_clock)

    def cache_features(self):
        pass

    def __setitem__(self, key, value):
        """ Feature does not have sub features """
        pass

    def __getitem__(self, item):
        """ Feature does not have sub features """
        raise KeyError

    def __getstate__(self):
        """ Fill in from the state """
        state = super(ReplicableFeature, self).__getstate__()
        state.update({
            "_value": self.value,
            "element": None
        })
        return state
