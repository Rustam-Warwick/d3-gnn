import abc
from abc import ABCMeta
from enum import Enum
from typing import TYPE_CHECKING, Tuple
from exceptions import OldVersionException
from asyncio import get_event_loop
from elements import GraphQuery, Op, query_for_part

if TYPE_CHECKING:
    from storage import BaseStorage
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

    def __init__(self, element_id: str, part_id=None, storage: "BaseStorage" = None) -> None:
        self.id: str = element_id
        self.storage: "BaseStorage" = storage
        self.part_id: int = part_id
        self.integer_clock = 0

    def add_storage_callback(self, storage: "BaseStorage"):
        self.storage = storage
        self.part_id = storage.part_id

    def __call__(self, rpc: "Rpc") -> bool:
        """ If this is master update and sync with replicas otherwise redirect to master node """
        is_updated = getattr(self, "%s" % (rpc.fn_name,))(*rpc.args, __call=True, **rpc.kwargs)
        return is_updated

    @abc.abstractmethod
    def update(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        """ General Update. Given new element make the update """
        pass

    def _sync(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        """ Sync element comes in. It should be commited if this is replica and incoming is a new version """
        assert self.is_replicable and self.state is ReplicaState.REPLICA, "Element should be replicas to receive sync messages"
        if self.integer_clock > new_element.integer_clock: raise OldVersionException
        self.integer_clock = new_element.integer_clock
        return self.update(new_element)

    def _update(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        """ Unconditional Update comes in commit only takes place in master nodes """
        if self.state is ReplicaState.REPLICA:
            query = GraphQuery(Op.UPDATE, new_element, self.master_part, True)
            self.storage.message(query)
            return False, self
        elif self.state is ReplicaState.MASTER:
            return self.update(new_element)

    @property
    @abc.abstractmethod
    def element_type(self) -> ElementTypes:
        """ Type of element Vertex, Feature, Edge """
        pass

    @property
    def state(self) -> ReplicaState:
        """ Replication State of this element """
        return ReplicaState.MASTER

    @property
    def is_replicable(self) -> bool:
        """ If this element is replicable/ can be replicated """
        return False

    @property
    def master_part(self) -> int:
        """ Master part of this GraphElement -1 if this is Master Part """
        return -1

    @property
    def replica_parts(self) -> list:
        """ Set of parts where this element is replicated """
        return list()

    def __setstate__(self, state: dict):
        if "storage" not in state: state['storage'] = None
        for i in state.values():
            #  Add element to feature
            if isinstance(i, GraphElement) and i.element_type == ElementTypes.FEATURE:
                i.element = self
        self.__dict__.update(state)

    def __getstate__(self):
        state = self.__dict__.copy()
        if "storage" in state: del state['storage']
        return state

    def __eq__(self, other):
        return self.id == other.id

    def sync_replicas(self):
        """ If this is master send SYNC to Replicas """
        if not self.is_replicable or not self.state == ReplicaState.MASTER: return
        query = GraphQuery(op=Op.SYNC, element=self, part=None, iterate=True)
        filtered_parts = map(lambda x: query_for_part(query, x),
                             filter(lambda x: x != self.part_id, self.replica_parts))
        for msg in filtered_parts:
            self.storage.message(msg)

    @property
    def is_ready(self) -> bool:
        return self.integer_clock > 0

    @property
    def features(self):
        """ Returns all the Feature attributes of this graphelement """
        from elements.feature import Feature
        a = list()
        for i in self.__dict__.items():
            if isinstance(i[1], Feature):
                # If this feature if GraphElement and there is no circular reference add
                a.append(i)
        return a
