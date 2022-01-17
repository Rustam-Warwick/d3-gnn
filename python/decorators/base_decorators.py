from elements import Rpc
from typing import TYPE_CHECKING
from elements import ReplicaState, GraphQuery, Op

if TYPE_CHECKING:
    from storage import BaseStorage
    from elements import GraphElement


def rpc(fn: object) -> callable:
    """ Wraps class instance methods to Rpc object
        Used to make send the RPC call over network in case the called element is replicated
    """

    def wrapper(self: "GraphElement", *args, __call=False, **kwargs):
        if __call is True:
            return fn(self, *args, **kwargs)
        remote_fn = Rpc(fn, self, *args, **kwargs)  # External Call
        return self(remote_fn)

    return wrapper
