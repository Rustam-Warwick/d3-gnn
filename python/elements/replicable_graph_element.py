from elements import ReplicaState, GraphElement, GraphQuery, Op, query_for_part
from typing import TYPE_CHECKING, Tuple
from copy import copy
from elements import ElementTypes

if TYPE_CHECKING:
    from elements.element_feature.set_feature import SetReplicatedFeature
    from elements.rpc import Rpc


class ReplicableGraphElement(GraphElement):
    """ General Graph element that is replicatable. Used in Vertex for now can be added for other things as well """

    def __init__(self, element_id: str = None, master: int = None, is_halo=False, *args, **kwargs):
        super(ReplicableGraphElement, self).__init__(element_id=element_id, *args, **kwargs)
        self._master = master  # Part_id of the master
        self._clock = 0  # Integer Clock
        self._halo = is_halo  # If this only a stub for master

    def __call__(self, rpc: "Rpc") -> Tuple[bool, "GraphElement"]:
        """ Wrap GraphElement call to have separate behavior for Replica & Master nodes """
        if self.state is ReplicaState.REPLICA:
            # Send this message to master node if it is replica
            query = GraphQuery(op=Op.RPC, element=rpc, part=self.master_part, iterate=True)
            self.storage.message(query)
            return False, self
        elif self.state is ReplicaState.MASTER:
            is_updated, elem = super(ReplicableGraphElement, self).__call__(rpc)
            if is_updated:
                self.sync_replicas()
            return is_updated, elem

    def create_element(self) -> bool:
        if self.state is ReplicaState.REPLICA: self._features.clear()  # Clear needed since it will be synced with
        # master anyway
        is_created = super(ReplicableGraphElement, self).create_element()  # Store addition and callbacks
        if not is_created: return is_created  # Failed to create or already existed in the database
        if self.state is ReplicaState.MASTER:
            from elements.element_feature.set_feature import SetReplicatedFeature
            self['parts'] = SetReplicatedFeature({self.storage.part_id}, is_halo=True)  # No need to replicate
            # replicas do not need this
        elif self.state is ReplicaState.REPLICA:
            # Sync this element with its master
            query = GraphQuery(Op.SYNC, self, self.master_part, True)
            self.storage.message(query)
        return is_created

    def sync_element(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        if self.state is ReplicaState.MASTER:
            # Add to parts of replicas and sync with this part
            self["parts"].add(new_element.part_id)
            self.sync_replicas(new_element.part_id, ignore_halo=False)
            return False, self
        elif self.state is ReplicaState.REPLICA:
            # Commit the update to replica
            if new_element.integer_clock <= self.integer_clock:
                # Old version exception
                # @todo I hope this is fine that I have cases where it enters here, not very often but still
                return False, self
            return self.update_element(new_element)

    def external_update(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        if self.state is ReplicaState.REPLICA:
            # External Updates only done in master part
            query = GraphQuery(Op.UPDATE, new_element, self.master_part, True)
            self.storage.message(query)
            return False, self
        elif self.state is ReplicaState.MASTER:
            is_updated, memento = super(ReplicableGraphElement, self).external_update(
                new_element)  # Basically calling update_element
            if is_updated:
                self.sync_replicas()
            return is_updated, memento

    def __iter__(self):
        """ Do not have parts in the iter  """
        return super(ReplicableGraphElement, self).__iter__()

    def sync_replicas(self, part_id=None, ignore_halo=True):
        """ Sending this element to all or some replicas. This element is shallow copied for operability """
        if self.state is not ReplicaState.MASTER or self.is_halo or len(self.replica_parts) == 0: return
        self.cache_features()
        cpy_self = copy(self)
        cpy_self._features.clear()
        for key, value in self:
            if ignore_halo and value.is_halo:
                continue
            ft_cpy = copy(value)
            ft_cpy.element = cpy_self
            cpy_self._features[key] = ft_cpy
            if ft_cpy.is_halo: ft_cpy._value = None

        query = GraphQuery(op=Op.SYNC, element=cpy_self, part=None, iterate=True)

        if part_id:
            self.storage.message(query_for_part(query, part_id))
        else:
            filtered_parts = map(lambda x: query_for_part(query, x), self.replica_parts)
            for msg in filtered_parts:
                self.storage.message(msg)

    @property
    def master_part(self) -> int:
        return self._master

    def get_integer_clock(self):
        return self._clock

    def set_integer_clock(self, value: int):
        self._clock = value

    def del_integer_clock(self):
        del self._clock

    integer_clock = property(get_integer_clock, set_integer_clock, del_integer_clock)

    @property
    def is_halo(self) -> bool:
        return self._halo

    @property
    def replica_parts(self) -> list:
        try:
            re: "SetReplicatedFeature" = self['parts']
            a = list(re.value)
            a.remove(self.part_id)
            return a
        except Exception:
            print("Replica Parts error")
            return list()

    @property
    def is_replicable(self) -> bool:
        return True

    def __getstate__(self):
        """ No need to serialize the parts """
        state = super(ReplicableGraphElement, self).__getstate__()
        state.update({
            "_master": self.master_part,
            "_clock": self.integer_clock,
            "_halo": self.is_halo
        })
        return state

    def __getmetadata__(self):
        metadata = super(ReplicableGraphElement, self).__getmetadata__()
        metadata.update({
            "_master": self.master_part,
            "_clock": self.integer_clock,
            "_halo": self.is_halo
        })
        return metadata
