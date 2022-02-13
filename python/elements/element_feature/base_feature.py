import abc
import copy
from abc import ABCMeta
from functools import cached_property
from typing import TYPE_CHECKING, Tuple
import re
from exceptions import NestedFeaturesException
from elements import ReplicableGraphElement, ElementTypes, GraphElement, ReplicaState


class ReplicableFeature(ReplicableGraphElement, metaclass=ABCMeta):
    """ Base class for all the features of Edge,Vertex or more upcoming updates
        Most of its features are combing from the associated GraphElement, whereas an ElementFeature is also a GraphElement
        @todo make this generic maybe ? To infer weather it is replicable or not: Hard to do without overheads
    """
    deep_copy_fields = ("_value",)  # Only deep copy _value if needed rest is just reference attachment

    def __init__(self, element: "GraphElement" = None, element_id: str = None, value: object = None, *args, **kwargs):
        self._value = value
        self.element: "GraphElement" = element
        element_id = "%s%s" % (ElementTypes.FEATURE.value, element_id)
        super(ReplicableFeature, self).__init__(element_id=element_id, *args, **kwargs)

    def create_element(self) -> bool:
        if self.attached_to[0] is ElementTypes.NONE:
            # Independent feature behave just like ReplicableGraphElements
            return super(ReplicableFeature, self).create_element()
        else:
            if self.element is None:
                # Make sure that element is here
                self.element = self.storage.get_element_by_id(self.attached_to[1])
            is_created = GraphElement.create_element(self)  # Omit all Replication Stuff
            if is_created and self.state is ReplicaState.MASTER:
                self.sync_replicas(skip_halos=False)

    def __call__(self, rpc: "Rpc") -> Tuple[bool, "GraphElement"]:
        """ Similar to sync_element we need to save the .element since integer_clock might change """
        is_updated, memento = super(ReplicableFeature, self).__call__(rpc)
        if is_updated and self.element is not None:
            self.storage.update_element(self.element)
        return is_updated, memento

    def update_element(self, new_element: "ReplicableFeature") -> Tuple[bool, "GraphElement"]:
        """ Similar to Graph Element  but added value swapping and no sub-feature checks """
        memento = copy.copy(self)  # .element field will be empty
        if new_element._value is None and self._value is None:
            is_updated = False
        elif new_element._value is None or self._value is None:
            is_updated = True
        else:
            is_updated = not self._value_eq_(self._value, new_element._value)
        if is_updated:
            self._value = new_element._value
            self.integer_clock = max(new_element.integer_clock, self.integer_clock)
            self.storage.update_element(self)
            self.storage.for_aggregator(lambda x: x.update_element_callback(self, memento))
        return is_updated, memento

    def sync_element(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        """ If directly syncing this feature parent vertex should also be updated """
        is_updated, memento = super(ReplicableFeature, self).sync_element(new_element)
        if is_updated and self.element is not None:
            self.storage.update_element(self.element)
        return is_updated, memento

    def external_update(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        """ We need to save the .element since integer_clock might change as well """
        is_updated, memento = super(ReplicableFeature, self).external_update(new_element)
        if is_updated and self.element is not None:
            self.storage.update_element(self.element)
        return is_updated, memento

    @abc.abstractmethod
    def _value_eq_(self, old_value, new_value) -> bool:
        """ Given @param(values) of 2 Features if they are equal  """
        pass

    @cached_property
    def field_name(self):
        """ Retrieve field name from the id """
        group = re.search("(\w+:)*(?P<feature_name>\w+)$", self.id[1:])
        if group:
            return group['feature_name']
        raise KeyError

    @cached_property
    def attached_to(self) -> Tuple["ElementTypes", str]:
        group = re.search("(?P<element_type>\w+):(?P<element_id>\w+):\w+", self.id[1:])
        if group:
            return ElementTypes(int(group['element_type'])), group['element_id']
        return (ElementTypes.NONE, None)  # Represents Standalone Feature

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
        return super(ReplicableFeature, self).replica_parts

    @property
    def is_halo(self) -> bool:
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

    def sync_replicas(self, part_id=None, skip_halos=True):
        """ Make sure is_halo Features send None and _value  """
        if self.attached_to[0] is ElementTypes.NONE:
            super(ReplicableFeature, self).sync_replicas(part_id, skip_halos)
        else:
            if self.is_halo:
                tmp = self._value
                self._value = None
                super(ReplicableFeature, self).sync_replicas(part_id, skip_halos)
                self._value = tmp
            else:
                super(ReplicableFeature, self).sync_replicas(part_id, skip_halos)

    def __setitem__(self, key, value):
        if self.attached_to[0] is ElementTypes.NONE:
            super(ReplicableFeature, self).__setitem__(key, value)
        else:

            raise NestedFeaturesException

    def __getitem__(self, item):
        if self.attached_to[0] is ElementTypes.NONE:
            super(ReplicableFeature, self).__setitem__(item)
        else:
            raise NestedFeaturesException

    def __getstate__(self):
        """ Fill in from the state """
        state = super(ReplicableFeature, self).__getstate__()
        state.update({
            "_value": self.value,
            "element": None  # No need to serialize element value
        })
        return state

    def __get_save_data__(self):
        meta_data = super(ReplicableFeature, self).__get_save_data__()
        meta_data.update({
            "_value": self._value
        })
        return meta_data
