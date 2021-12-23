from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elements import GraphElement


class Op(Enum):
    NONE = 0
    ADD = 1
    REMOVE = 2
    UPDATE = 3
    SYNC = 4
    AGG = 5


class GraphQuery:
    def __init__(self, op: Op, element: "GraphElement", part: int = None) -> None:
        self.op: Op = op
        self.part: int = part
        self.element: "GraphElement" = element


def query_for_part(query: GraphQuery, part: int) -> GraphQuery:
    """Returns a new GraphQuery but directed to a different part"""
    return GraphQuery(query.op, part, query.element)
