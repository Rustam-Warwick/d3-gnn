import abc
from abc import ABCMeta
from typing import TYPE_CHECKING
from elements import GraphElement, ElementTypes, Rpc, GraphQuery, Op
from elements.graph_query import query_for_part

if TYPE_CHECKING:
    from elements import ReplicaState, ReplicableGraphElement


class Feature(GraphElement, metaclass=ABCMeta):
    """ Base class for all the feautres of Edge,Vertex or more
        Implemented in a way that Features can exist without being attached to element al long as they have unique ids
    """

    def __init__(self, field_name: str, element: "GraphElement", value: object = None, *args, **kwargs):
        feature_id = "%s:%s:%s" % (element.element_type.value, element.id, field_name)
        self.value = value
        self.element: "GraphElement" = element
        super(Feature, self).__init__(element_id=feature_id, *args, **kwargs)

    def __call__(self, rpc: "Rpc"):
        """ Find the private RPC function of this element and call with the given arguments"""
        getattr(self, "_%s" % (rpc.fn_name,))(*rpc.args, **rpc.kwargs)
        # @todo Add condition to check if there is actually an update
        self.__sync()  # Sync with replicas

    @property
    def element_type(self):
        return ElementTypes.FEATURE

    def __getstate__(self):
        state = super(Feature, self).__getstate__()
        del state['element']
        return state

    def __sync(self):
        """ If this is master send SYNC to Replicas """
        from elements import ReplicaState
        el: "ReplicableGraphElement" = self.element
        if el.state == ReplicaState.MASTER:
            query = GraphQuery(op=Op.SYNC, element=self, part=None, iterate=True)
            filtered_parts = map(lambda x: query_for_part(query, x), filter(lambda x: x != self.part_id, el.parts.value))
            for msg in filtered_parts: self.storage.message(msg)
