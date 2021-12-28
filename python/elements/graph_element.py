import abc
from abc import ABCMeta
from enum import Enum
from typing import TYPE_CHECKING
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

    def pre_add_storage_callback(self, storage: "BaseStorage"):
        self.storage = storage
        self.part_id = storage.part_id
        [x[1].pre_add_storage_callback(storage) for x in self.features]

    def post_add_storage_callback(self, storage: "BaseStorage"):
        [x[1].post_add_storage_callback(storage) for x in self.features]
        pass

    def __call__(self, rpc: "Rpc") -> bool:
        """ If this is master update and sync with replicas otherwise redirect to master node """
        if self.state == ReplicaState.MASTER:
            # This is already the master node so just commit the messages
            is_updated = getattr(self, "_%s" % (rpc.fn_name,))(*rpc.args, **rpc.kwargs)
            if is_updated: self.sync_replicas()
            return is_updated
        elif self.state == ReplicaState.REPLICA:
            # Send this message to master node
            query = GraphQuery(op=Op.RPC, element=rpc, part=self.master_part, iterate=True)
            self.storage.message(query)
            return False

    @abc.abstractmethod
    def update(self, new_element: "GraphElement") -> bool:
        """ Given new value of this element update the necessary states """
        pass

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
                             filter(lambda x: x is not self.part_id, self.replica_parts))
        for msg in filtered_parts:
            self.storage.message(msg)

    @property
    def child_elements(self):
        """ Returns all child elements/attributes which are also Features """
        from elements.feature import Feature
        a = list()
        if isinstance(self, Feature): return a  # Stop at Feature since circular reference otherwise
        for i in self.__dict__.values():
            if isinstance(i, GraphElement):
                # If this feature if GraphElement and there is no circular reference add
                a.append(i)
        return a

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
