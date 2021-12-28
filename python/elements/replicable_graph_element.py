from elements import ReplicaState
from elements.graph_element import GraphElement
from elements.feature.set_feature import PartSetFeature
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage import BaseStorage


class ReplicableGraphElement(GraphElement):
    def __init__(self, master: int = None, *args, **kwargs):
        super(ReplicableGraphElement, self).__init__(*args, **kwargs)
        self.master = master
        self.parts = PartSetFeature(field_name="parts", element=self)  # parts where this guy is replicated

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
    def master_part(self) -> int:
        return self.master

    @property
    def replica_parts(self) -> list:
        re: set = self.parts.value
        a = list(re)
        a.remove(self.part_id)
        return a

    @property
    def is_replicable(self) -> bool:
        return True
