import abc
from abc import ABCMeta
from typing import TYPE_CHECKING
from elements import GraphElement, ElementTypes, ReplicableGraphElement, ReplicaState

if TYPE_CHECKING:
    from storage.gnn_layer import GNNLayerProcess


class BaseAggregator(ReplicableGraphElement, metaclass=ABCMeta):
    """ Base class for all the Aggregators in the storage. Subclass of Replicable Graph Element. Only different no
    sync on create!!! """

    def __init__(self, element_id: str, *args, **kwargs):
        new_element_id = "%s%s" % (ElementTypes.AGG.value, element_id)
        super(BaseAggregator, self).__init__(master=0, element_id=new_element_id, *args, **kwargs)

    def create_element(self) -> bool:
        is_created = GraphElement.create_element(self)  # Store addition and callbacks
        if not is_created: return is_created  # Failed to create or already existed in the database
        if self.state is ReplicaState.MASTER:
            from elements.element_feature.set_feature import SetReplicatedFeature
            self['parts'] = SetReplicatedFeature(set(range(0, self.storage.parallelism)),
                                                 is_halo=True)  # No need to replicate
            # replicas do not need this
        return is_created

    @property
    def element_type(self) -> ElementTypes:
        return ElementTypes.AGG

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        """ Called with query in *args, when there is associated AGG event coming in with corresponding aggregator
        name """
        pass

    def add_element_callback(self, element: "GraphElement"):
        """ Callback after Graph Element is added """
        pass

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        """ When a GraphElement is updated, SYNC, UPDATE, RPC  """
        if element.element_type is ElementTypes.FEATURE and element.attached_to[1] == self.id:
            self._features[element.field_name] = element
            element.element = self
