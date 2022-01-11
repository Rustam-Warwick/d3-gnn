import abc
import copy
from abc import ABCMeta
from enum import Enum
from typing import TYPE_CHECKING, Tuple
from exceptions import OldVersionException, NotUsedOnReplicaException
from asyncio import get_event_loop
from elements import GraphQuery, Op, query_for_part

if TYPE_CHECKING:
    from storage.process_fn import GraphStorageProcess
    from elements.element_feature import ElementFeature
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

    def __init__(self, element_id: str = None, part_id=None, storage: "GraphStorageProcess" = None) -> None:
        self.id: str = element_id
        self.part_id: int = part_id # None represents the part id of the storage engine
        self.storage: "GraphStorageProcess" = storage # Storage
        self._features = dict() # Cached version of the element features

    def add_storage_callback(self, storage: "GraphStorageProcess"):
        """ Populate some fields """
        self.storage = storage
        self.part_id = storage.part_id

    def __call__(self, rpc: "Rpc") -> bool:
        is_updated = getattr(self, "%s" % (rpc.fn_name,))(*rpc.args, __call=True, **rpc.kwargs)
        if is_updated:
            self.storage.update(self)
            self.storage.for_aggregator(lambda x: x.update_element_callback(self))
            if self.state is ReplicaState.MASTER: self.sync_replicas()
        return is_updated

    def update(self, new_element: "GraphElement") -> bool:
        """ General Update function. Take all the overlapping Features and call update on them """
        memento = copy.copy(self)  # Old Version
        is_updated = False
        for name, value in new_element.features:
            is_updated |= self[name].update(value)  # Call Update on the Features

        self.integer_clock = max(self.integer_clock, new_element.integer_clock)
        if is_updated:
            self.storage.update(self)
            self.storage.for_aggregator(lambda x: x.update_element_callback(self, memento))
            if self.state is ReplicaState.MASTER: self.sync_replicas()
        return is_updated

    def _sync(self, new_element: "GraphElement") -> bool:
        """ Sync should be called only on replicable features.
            Master -> Send current state of graphelement to replica
            Replica -> Update the graphElement if integer clock is behind
        """
        if not self.is_replicable: raise NotUsedOnReplicaException
        if self.state is ReplicaState.MASTER:
            self["parts"].add(new_element.part_id)
            return False
        elif self.state is ReplicaState.REPLICA:
            if new_element.integer_clock < self.integer_clock: raise OldVersionException
            return self.update(new_element)

    def _update(self, new_element: "GraphElement") -> bool:
        """ Unconditional Update comes in commit only takes place in master nodes """
        if self.state is ReplicaState.REPLICA:
            query = GraphQuery(Op.UPDATE, new_element, self.master_part, True)
            self.storage.message(query)
            raise NotUsedOnReplicaException
        elif self.state is ReplicaState.MASTER:
            return self.update(new_element)

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
        return self.integer_clock > 0

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

    @property
    def features(self):
        """ Returns all the Feature attributes of this Graph Element """
        return self._features.items()

    def __eq__(self, other):
        return self.id == other.id

    def __setstate__(self, state: dict):
        """  """
        if "storage" not in state: state['storage'] = None

        for i in state['_features'].values():
            #  Add element to element_feature
            if isinstance(i, GraphElement) and i.element_type == ElementTypes.FEATURE:
                i.element = self
        self.__dict__.update(state)

    def __getstate__(self):
        """ Serialization, remove storage reference. <id, part_id, _features>.
            Note that _features which are fetched from storage are going to be serialized
         """
        state = self.__dict__.copy()
        if "storage" in state: del state['storage']
        return state

    def __getitem__(self, item):
        """ Get a Feature from this vertex """
        item = None
        if item in self._features:
            item = self._features[item]
            item.element = self
            return item
        elif self.storage:
            item = self.storage.get_feature("%s:%s:%s" % (self.element_type.value, self.id, item))
            item.element = self
            return item
        raise KeyError

    def __setitem__(self, key, value: "ElementFeature"):
        """ Set a Feature to this vertex """
        value.id = "%s:%s:%s" % (self.element_type.value, self.id, key)  # Set Id
        value.element = self  # Set Element
        self._features[key] = value
        if self.storage:
            # Setting element which is attached to graph storage
            self.storage._add_feature(value)

    def cache_features(self):
        element = self.storage.get_element(self.id, True)
        self._features = element._features

    def sync_replicas(self):
        """ If this is master send SYNC to Replicas """
        if not self.is_replicable or not self.state == ReplicaState.MASTER: return
        self.cache_features()
        query = GraphQuery(op=Op.SYNC, element=self, part=None, iterate=True)
        filtered_parts = map(lambda x: query_for_part(query, x),
                             filter(lambda x: x != self.storage.part_id, self.replica_parts))
        for msg in filtered_parts:
            self.storage.message(msg)
