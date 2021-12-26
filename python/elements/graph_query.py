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
    RPC = 5
    AGG = 6



class GraphQuery:
    def __init__(self, op: Op, element: "GraphElement", part: int = None, iterate=False) -> None:
        self.op: Op = op
        self.part: int = part
        self.element: "GraphElement" = element
        self.iterate = iterate


def query_for_part(query: GraphQuery, part: int) -> GraphQuery:
    """Returns a new GraphQuery but directed to a different part"""
    return GraphQuery(op=query.op, part=part, element=query.element,iterate=query.iterate)
