from elements.feature import Feature
import elements.replicable_graph_element as rge
from elements import Rpc, GraphQuery, Op
from typing import TYPE_CHECKING
from elements.graph_query import query_for_part

if TYPE_CHECKING:
    from elements import ReplicableGraphElement


class ReplicableFeature(Feature):
    def __init__(self, *args, **kwargs):
        super(ReplicableFeature, self).__init__(*args, **kwargs)

    def update(self, new_element: "ReplicableFeature"):
        # @todo If needed add integer clock
        super().update(new_element)

    def __call__(self, rpc: "Rpc") -> bool:
        el: ReplicableGraphElement = self.element
        if el.state == rge.ReplicaState.MASTER:
            # This is already the master node so just commit the messages
            is_updated = super(ReplicableFeature, self).__call__(rpc)
            if is_updated: self.__sync()
            return is_updated
        elif el.state == rge.ReplicaState.REPLICA:
            # Send this message to master node
            query = GraphQuery(op=Op.RPC, element=rpc, part=el.master_part, iterate=True)
            self.storage.message(query)
            return False

    def __sync(self):
        """ If this is master send SYNC to Replicas """
        from elements import ReplicaState
        el: "ReplicableGraphElement" = self.element
        if el.state == ReplicaState.MASTER:
            query = GraphQuery(op=Op.SYNC, element=self, part=None, iterate=True)
            filtered_parts = map(lambda x: query_for_part(query, x),
                                 filter(lambda x: x != self.part_id, el.parts.value))
            for msg in filtered_parts: self.storage.message(msg)

    @property
    def is_replicable(self) -> bool:
        return True

