from .base_partitioner import BasePartitioner
from random import randint
from elements import ElementTypes
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elements import GraphQuery
    from elements.edge import BaseEdge
    from elements.vertex import BaseVertex
    from elements.element_feature import ReplicableFeature


class RandomPartitioner(BasePartitioner):
    def __init__(self, *args, **kwargs):
        super(RandomPartitioner, self).__init__(*args, **kwargs)
        self.masters = dict()

    def is_parallel(self):
        return False

    def add_to_dict(self, vertex_id: str, value: int):
        if vertex_id not in self.masters:
            self.masters[vertex_id] = value

    def assign_vertex_state(self, vertex: "BaseVertex"):
        vertex._master = self.masters[vertex.id]

    def map(self, value: "GraphQuery"):

        if value.element.element_type is ElementTypes.EDGE:
            """ Edges are unique so need to assign randomly to a new part """
            value.part = randint(0, self.partitions - 1)
            edge: "BaseEdge" = value.element
            self.add_to_dict(edge.source.id, value.part)
            self.add_to_dict(edge.destination.id, value.part)
            self.assign_vertex_state(edge.source)
            self.assign_vertex_state(edge.destination)
        if value.element.element_type is ElementTypes.VERTEX:
            """Vertex Updates might contain Features as well so test if it is already push to master, 
            else randomly assign a master part """
            part = randint(0, self.partitions - 1)
            self.add_to_dict(value.element.id, part)
            part = self.masters[value.element.id]
            self.assign_vertex_state(value.element)
            value.part = part

        return value
