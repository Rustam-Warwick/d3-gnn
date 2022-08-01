from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elements import GraphElement


class RPCDestination(Enum):
    SELF = 0
    MASTER = 1
    REPLICAS = 2
    ALL = 3
    CUSTOM = 4


class Rpc:
    def __init__(self, fn, element: "GraphElement", is_procedure: bool = False, *args, **kwargs):
        self.id = element.id
        self.is_procedure = is_procedure
        self.fn_name = fn.__name__
        self.args = args
        self.kwargs = kwargs
