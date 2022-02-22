from elements import Rpc
from typing import TYPE_CHECKING
from elements import ReplicaState, GraphQuery, Op, IterationState, query_for_part
from elements.rpc import RPCDestination

if TYPE_CHECKING:
    from elements import GraphElement


def rpc(is_procedure: bool = False, iteration=IterationState.ITERATE, destination=RPCDestination.MASTER) -> callable:
    """
        Calls the function with the Rpc method. So this is normal RPC That edits something
        iteration -> Forward, Backward, Iterate. In which direction this RPC should be directed
        destination -> How the destination part is being selected
        is_procedure -> Is this RPC a procedure(does not alter state) or function that changes the state

    """

    def result(fn):
        def wrapper(self: "GraphElement", *args, __call=False, __iteration=iteration, __destination=destination,
                    __parts=[], **kwargs):

            if "part_id" not in kwargs:
                kwargs['part_id'] = self.part_id
            if 'part_version' not in kwargs:
                kwargs['part_version'] = self.storage.part_version

            if __call is True:
                return fn(self, *args, **kwargs)
            remote_fn = Rpc(fn, self, is_procedure, *args, **kwargs)  # External Call
            query = GraphQuery(op=Op.RPC, element=remote_fn)
            query.iteration_state = __iteration
            parts = []
            if __destination is RPCDestination.SELF:
                parts = [self.part_id]
            elif __destination is RPCDestination.MASTER:
                parts = [self.master_part]
            elif __destination is RPCDestination.REPLICAS:
                parts = self.replica_parts
            elif __destination is RPCDestination.ALL:
                parts = [*self.replica_parts, self.master_part]
            elif __destination is RPCDestination.CUSTOM:
                parts = __parts
            for i in parts:
                if i == self.part_id and query.iteration_state is IterationState.ITERATE:
                    self(remote_fn)
                    continue
                self.storage.message(query_for_part(query, i))

        return wrapper

    return result
