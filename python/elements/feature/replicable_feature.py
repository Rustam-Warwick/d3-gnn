from elements.feature import Feature
import elements.replicable_graph_element as rge
from elements import Rpc, GraphQuery, Op
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elements import ReplicableGraphElement


class ReplicableFeature(Feature):
    def __init__(self, *args, **kwargs):
        super(ReplicableFeature, self).__init__(*args, **kwargs)

    def __call__(self, rpc: "Rpc"):
        el: ReplicableGraphElement = self.element
        if el.state == rge.ReplicaState.MASTER:
            # This is already the master node so just commit the messages
            super(ReplicableFeature, self).__call__(rpc)
        elif el.state == rge.ReplicaState.REPLICA:
            # Send this message to master node
            query = GraphQuery(op=Op.RPC, element=rpc, part=el.master_part, iterate=True)
            self.storage.message(query)
