import abc
import copy
from abc import ABCMeta
from enum import Enum
from typing import TYPE_CHECKING, Tuple, Dict
from exceptions import OldVersionException, NotUsedOnReplicaException, GraphElementNotFound
from asyncio import get_event_loop
from elements import GraphQuery, Op, query_for_part

if TYPE_CHECKING:
    from storage.process_fn import GraphStorageProcess
    from elements.element_feature import ReplicableFeature
    from elements import Rpc


class ReplicaState(Enum):
    UNDEFINED = 0
    REPLICA = 1
    MASTER = 2


class ElementTypes(Enum):
    VERTEX = 0
    EDGE = 1
    FEATURE = 2


class GraphElement(metaclass=ABCMeta):
    """GraphElement is the main parent class of all Vertex, Edge, Feature classes"""
    copy_fields = ("_features",)

    def __init__(self, element_id: str = None, part_id=None, storage: "GraphStorageProcess" = None) -> None:
        self.id: str = element_id
        self.part_id: int = part_id  # None represents the part id of the storage engine
        self.storage: "GraphStorageProcess" = storage  # Storage
        self._features: Dict[str, "ReplicableFeature"] = dict()  # Cached version of the element features

    def __call__(self, rpc: "Rpc") -> Tuple[bool,"GraphElement"]:
        new_element = copy.deepcopy(self)
        getattr(new_element, "%s" % (rpc.fn_name,))(*rpc.args, __call=True, **rpc.kwargs)
        return self.update_element(new_element)

    def create_element(self) -> bool:
        """ Save this GraphELement in memory """
        is_created = self.storage.add_element(self)
        if not is_created:return is_created
        for key, value in self:
            GraphElement.create_element(value) # Call with the graph element create_element
        if is_created:
            self.storage.for_aggregator(lambda x: x.add_element_callback(self))
        return is_created

    def update_element(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        """ General Update function """
        memento = copy.copy(self)
        is_updated = False
        for name, value in new_element:
            is_updated_feature, memento_feature = self[name].update_element(value)  # Call Update on the Features
            is_updated |= is_updated_feature
            memento._features[name] = memento_feature  # Populate this to not trigger the storage update
        if is_updated:
            self.integer_clock = max(new_element.integer_clock, self.integer_clock)
            self.storage.update(self)
            self.storage.for_aggregator(lambda x: x.update_element_callback(self, memento))
        return is_updated, memento

    def sync_element(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        """ Sync this GraphElement Implemented for Replicable Graph Element """
        pass

    def external_update(self, new_element:"GraphElement") -> Tuple[bool, "GraphElement"]:
        """ Unconditional Update function """
        self.update_element(new_element)

    def __iter__(self):
        """ Iterate over the attached features """
        return iter(self._features.items())

    @property
    @abc.abstractmethod
    def element_type(self) -> ElementTypes:
        """ Type of element Vertex, Feature, Edge """
        pass

    @property
    def is_replicable(self) -> bool:
        """ If this element is replicable/ can be replicated """
        return False

    @property
    def master_part(self) -> int:
        """ Master part of this GraphElement -1 if this is Master Part """
        return -1

    @property
    def state(self) -> ReplicaState:
        """ State of this GraphElements Instance """
        if self.master_part is None:
            return ReplicaState.UNDEFINED
        if self.master_part == -1:
            return ReplicaState.MASTER
        return ReplicaState.REPLICA

    @property
    def replica_parts(self) -> list:
        """ Set of parts where this element is replicated """
        return list()

    @property
    def is_initialized(self) -> bool:
        """ """
        return self.state is ReplicaState.MASTER or self.integer_clock > 0

    @property
    def is_waiting(self) -> bool:
        """ If this graph element is replica and is waiting for update this is going to happen """
        return self.integer_clock < 0

    @property
    def is_halo(self) -> bool:
        return False

    def get_integer_clock(self):
        return 1

    def set_integer_clock(self, value: int):
        pass

    def del_integer_clock(self):
        pass

    integer_clock = property(get_integer_clock, set_integer_clock, del_integer_clock)

    def __eq__(self, other):
        return self.id == other.id

    def __deepcopy__(self, memodict={}):
        cls = self.__class__
        result = cls.__new__(cls)
        memodict[id(self)] = result
        for k, v in self.__dict__.items():
            if k in self.copy_fields:
                setattr(result, k, copy.deepcopy(v, memodict))
            else:
                setattr(result, k, v)
        return result

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __setstate__(self, state: dict):
        """  """
        state['storage'] = None
        if "_features" in state:
            for i in state['_features'].values():
                #  Add element to element_feature
                if isinstance(i, GraphElement) and i.element_type == ElementTypes.FEATURE:
                    i.element = self
        self.__dict__.update(state)

    def __getstate__(self):
        """ Serialization, remove storage reference. <id, part_id, _features>.
            Note that _features which are fetched from storage are going to be serialized
         """
        return {
            "id": self.id,
            "part_id": self.part_id,
            "_features": self._features,
            "storage": None
        }

    def __getitem__(self, key) -> "ReplicableFeature":
        """ Get a Feature from this vertex """
        try:
            if key in self._features:
                item = self._features[key]
                item.element = self
                return item
            elif self.storage:
                item = self.storage.get_feature("%s:%s:%s" % (self.element_type.value, self.id, key))
                item.element = self
                self._features[key] = item  # Cache for future usage
                return item
        except GraphElementNotFound:
            raise KeyError

    def __setitem__(self, key, value: "ReplicableFeature"):
        """ Set a Feature to this vertex """
        value.id = "%s:%s:%s" % (self.element_type.value, self.id, key)  # Set Id
        value.element = self  # Set Element
        value.part_id = self.part_id
        value.storage = self.storage
        self._features[key] = value
        if self.storage:
            # Setting element which is attached to graph storage
            value.create_element()

    def cache_features(self):
        features = self.storage.get_features(self.element_type, self.id)
        for key, value in features.items():
            value.element = self
            self._features[key] = value

    def attach_storage(self, storage: "GraphStorageProcess"):
        """ Simply attach the storage to this element """
        self.storage = storage
        self.part_id = storage.part_id
        for feature in self._features.values():
            feature.attach_storage(storage)

    def detach_storage(self):
        """ Remove the storage from this element """
        self.storage = None
        for feature in self._features.values():
            feature.detach_storage()

    def sync_replicas(self):
        """ If this is master send SYNC to Replicas """
        self.cache_features()
        query = GraphQuery(op=Op.SYNC, element=self, part=None, iterate=True)
        filtered_parts = map(lambda x: query_for_part(query, x),
                             filter(lambda x: x != self.storage.part_id, self.replica_parts))
        for msg in filtered_parts:
            self.storage.message(msg)
