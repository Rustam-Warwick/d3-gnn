from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elements import GraphElement


class Rpc:
    def __init__(self, fn, element: "GraphElement", *args, **kwargs):
        self.id = element.id
        self.fn_name = fn.__name__
        self.args = args
        self.kwargs = kwargs
