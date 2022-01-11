import operator

from elements import ReplicaState
import copy
from elements.graph_element import GraphElement, GraphQuery, Op
from elements.element_feature.set_feature import PartSetElementFeature
from typing import TYPE_CHECKING, Tuple
from exceptions import OldVersionException

if TYPE_CHECKING:
    from storage.process_fn import GraphStorageProcess


class ReplicableGraphElement(GraphElement):
    def __init__(self, master: int = None, is_halo=False, *args, **kwargs):
        super(ReplicableGraphElement, self).__init__(*args, **kwargs)
        self.master = master
        self._clock = 0
        self._halo = is_halo

    def add_storage_callback(self, storage: "GraphStorageProcess"):
        """ Fill up the PartSetElementFeature """
        super(ReplicableGraphElement, self).add_storage_callback(storage)
        if self.state is ReplicaState.MASTER:
            """ If master create Part Set """
            self["parts"] = PartSetElementFeature({self.storage.part_id})
        elif self.state is ReplicaState.REPLICA:
            """ Otherwise send sync to Master """
            query = GraphQuery(Op.SYNC, self, self.master_part, True)
            self.storage.message(query)








    @property
    def master_part(self) -> int:
        return self.master

    def get_integer_clock(self):
        return self._clock

    def set_integer_clock(self, value: int):
        self._clock = value

    def del_integer_clock(self):
        del self._clock

    @property
    def is_halo(self) -> bool:
        return self._halo

    @property
    def replica_parts(self) -> list:
        re: "PartSetElementFeature" = self['parts']
        a = list(re.value)
        a.remove(self.part_id)
        return a

    @property
    def is_replicable(self) -> bool:
        return True

    def __getstate__(self):
        """ No need to serialize the parts """
        state = super(ReplicableGraphElement, self).__getstate__()
        if "parts" in state["_features"]:
            del state["_features"]["parts"]
        return state