from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elements import GraphElement


class Op(Enum):
    NONE = 0  # Not useful
    ADD = 1  # Element being added
    REMOVE = 2  # Element being removed
    UPDATE = 3  # Element being updated
    SYNC = 4  # Replicated Element being Synced i.e. Master - Replica communication within 1 parallel instance
    RPC = 5  # RPC Call
    AGG = 6  # Call for some attached function aggregator .run() method


class IterationState(Enum):
    FORWARD = False  # GraphQuery should be sent forward as normal
    ITERATE = True   # GraphQuery should be iterating within one parallel instance
    BACKWARD = None  # GraphQuery should be sent backwards, used for back-prop


class GraphQuery:
    """ Main Query that is being passed around
        @NOTE That element is not GraphElement for RPC and for AGG
     """

    def __init__(self, op: Op, element: "GraphElement", part: int = None, iterate=False, aggregator_name=None) -> None:
        self.op: Op = op
        self.part: int = part
        self.element: "GraphElement" = element
        self.iterate = iterate
        self.aggregator_name = aggregator_name  # In case this is AGG Op

    @property
    def is_topology_change(self):
        return self.op is Op.ADD or self.op is Op.REMOVE

    def get_iteration_state(self) -> IterationState:
        if self.iterate is False:
            return IterationState.FORWARD
        elif self.iterate is True:
            return IterationState.ITERATE
        else:
            return IterationState.BACKWARD

    def set_iteration_state(self, state: IterationState):
        self.iterate = state.value

    def del_iteration_state(self):
        pass

    iteration_state = property(get_iteration_state, set_iteration_state, del_iteration_state)

    def __str__(self):
        return self.op.__str__() + self.element.__str__()


def query_for_part(query: GraphQuery, part: int) -> GraphQuery:
    """Returns a new GraphQuery but directed to a different part"""
    return GraphQuery(op=query.op, part=part, element=query.element, iterate=query.iterate,
                      aggregator_name=query.aggregator_name)
