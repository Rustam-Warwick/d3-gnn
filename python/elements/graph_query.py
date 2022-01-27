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
    RPC = 5  # RPC Call from Replica to Master node
    AGG = 6  # Call for some attached function aggregator .run()


class IterationState(Enum):
    FORWARD = False
    ITERATE = True
    BACKWARD = None


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

    def get_is_train(self) -> bool:
        return self.op is Op.AGG and self.aggregator_name == 'trainer'

    def set_is_train(self, state: bool):
        if state is True:
            self.op = Op.AGG
            self.aggregator_name = "trainer"  # Special Reserved Aggregator function for training

    def del_is_train(self):
        pass

    is_train = property(get_is_train, set_is_train, del_is_train)

    def __str__(self):
        return self.op.__str__() + self.element.__str__()


def query_for_part(query: GraphQuery, part: int) -> GraphQuery:
    """Returns a new GraphQuery but directed to a different part"""
    return GraphQuery(op=query.op, part=part, element=query.element, iterate=query.iterate, aggregator_name=query.aggregator_name)
