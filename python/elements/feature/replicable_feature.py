from elements.feature import Feature
import elements.replicable_graph_element as rge
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elements import ReplicableGraphElement


class ReplicableFeature(Feature):
    def __init__(self, *args, **kwargs):
        super(ReplicableFeature, self).__init__(*args, **kwargs)

    def __call__(self, lambda_fn):
        el: ReplicableGraphElement = self.element
        if el.state == rge.ReplicaState.MASTER:
            super(ReplicableFeature, self).__call__(lambda_fn)
        elif el.state == rge.ReplicaState.REPLICA:
            self.storage.msg_master(lambda_fn)
