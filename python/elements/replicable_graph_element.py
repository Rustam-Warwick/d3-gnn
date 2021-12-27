from elements.graph_element import GraphElement
from enum import Enum
from elements.feature.set_feature import SetReplicableFeature
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage import BaseStorage


class ReplicaState(Enum):
    UNDEFINED = 0
    REPLICA = 1
    MASTER = 2


class ReplicableGraphElement(GraphElement):
    def __init__(self, master_part: int = None, *args, **kwargs):
        super(ReplicableGraphElement, self).__init__(*args, **kwargs)
        self.master_part = master_part
        self.parts = SetReplicableFeature(field_name="parts", element=self)  # parts where this guy is replicated

    def pre_add_storage_callback(self, storage: "BaseStorage"):
        super(ReplicableGraphElement, self).pre_add_storage_callback(storage)
        self.parts.add(self.part_id)  # If Replica it syncs automatically

    @property
    def state(self) -> ReplicaState:
        if self.master_part is None:
            return ReplicaState.UNDEFINED
        if self.master_part == -1:
            return ReplicaState.MASTER
        return ReplicaState.REPLICA

    @property
    def is_replicable(self) -> bool:
        return True
