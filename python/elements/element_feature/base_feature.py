import abc
import copy
from abc import ABCMeta
from typing import TYPE_CHECKING, Tuple
from exceptions import NotSupported
import re
from elements import ReplicableGraphElement, ElementTypes, ReplicaState, Op, GraphQuery, GraphElement


class ReplicableFeature(ReplicableGraphElement, metaclass=ABCMeta):
    """ Base class for all the features of Edge,Vertex or more upcoming updates
        Most of its features are combing from the associated GraphElement, whereas an ElementFeature is also a GraphElement
        @todo make this generic maybe ? To infer weather it is replicable or not: Hard to do without overheads
    """
    deep_copy_fields = ("_value",)  # Only deep copy _value if needed rest is just reference attachment

    def __init__(self, element: "GraphElement" = None, value: object = None, *args, **kwargs):
        self._value = value
        self.element: "GraphElement" = element
        super(ReplicableFeature, self).__init__(*args, **kwargs)

    def create_element(self) -> bool:
        """ Just a normal Creation without any Features """
        self._features.clear()  # No sub Features allowed for Features
        return GraphElement.create_element(self)

    def __call__(self, rpc: "Rpc") -> Tuple[bool, "GraphElement"]:
        """ Similar to sync_element we need to save the .element since integer_clock might change """
        is_changed, memento = super(ReplicableFeature, self).__call__(rpc)
        if is_changed:
            self.storage.update_element(self.element)
        return is_changed, memento

    def update_element(self, new_element: "ReplicableFeature") -> Tuple[bool, "GraphElement"]:
        """ Similar to Graph Element  but added value swapping and no sub-feature checks """
        memento = copy.copy(self)  # .element field will be empty
        is_updated = not self._value_eq_(self._value, new_element._value)
        if is_updated:
            self._value = new_element._value
            self.integer_clock = max(new_element.integer_clock, self.integer_clock)
            self.storage.update_element(self)
            self.storage.for_aggregator(lambda x: x.update_element_callback(self, memento))
        return is_updated, memento

    def sync_element(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        """ If directly syncing this feature parent vertex should also be updated """
        if self.state is ReplicaState.REPLICA:
            is_changed, memento = super(ReplicableFeature, self).sync_element(new_element)
            if is_changed:
                self.storage.update_element(self.element)
            return is_changed, memento
        raise NotSupported  # Not implemented feature resolution yet :>
        # @todo Should we implement smt like this that calls sync_replicas()

    def external_update(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        """ We need to save the .element since integer_clock might change as well """
        is_changed, memento = super(ReplicableFeature, self).external_update(new_element)
        if is_changed:
            self.storage.update_element(self.element)
        return is_changed, memento

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
        return super(ReplicableFeature, self).master_part

    @property
    def replica_parts(self) -> list:
        if self.element:
            return self.element.replica_parts
        return list()

    @property
    def is_halo(self) -> bool:
        if self.element:
            return self.element.is_halo
        return super(ReplicableFeature, self).is_halo

    def get_integer_clock(self):
        if self.element:
            return self.element.get_integer_clock()
        return super(ReplicableFeature, self).get_integer_clock()

    def set_integer_clock(self, value: int):
        if self.element:
            self.element.set_integer_clock(value)
            return
        super(ReplicableFeature, self).set_integer_clock(value)

    def del_integer_clock(self):
        if self.element:
            self.element.del_integer_clock()
            return
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
            "element": None  # No need to serialize element value
        })
        return state
