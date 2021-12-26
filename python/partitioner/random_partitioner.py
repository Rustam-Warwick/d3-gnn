from .base_partitioner import BasePartitioner
from random import randint
from elements import ElementTypes
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elements import GraphQuery
    from elements.edge import BaseEdge
    from elements.vertex import BaseVertex


class RandomPartitioner(BasePartitioner):
    def __init__(self, *args, **kwargs):
        super(RandomPartitioner, self).__init__(*args, **kwargs)
        self.masters = dict()

    def is_parallel(self):
        return False

    def add_to_dict(self, vertex_id: str, value: int):
        if vertex_id not in self.masters:
            self.masters[vertex_id] = value

    def assign_vertex_state(self, vertex: "BaseVertex", part: int):
        if self.masters[vertex.id] == part:
            vertex.master_part = -1
        else:
            vertex.master_part = part

    def map(self, value: "GraphQuery"):
        if value.part is None:
            value.part = randint(0, self.partitions - 1)

        if value.element.element_type is ElementTypes.EDGE:
            edge: "BaseEdge" = value.element
            self.add_to_dict(edge.source.id, value.part)
            self.add_to_dict(edge.destination.id, value.part)
            self.assign_vertex_state(edge.source, value.part)
            self.assign_vertex_state(edge.destination, value.part)

        return value
