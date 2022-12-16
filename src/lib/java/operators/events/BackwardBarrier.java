package operators.events;

import elements.enums.MessageDirection;

public class BackwardBarrier extends BaseOperatorEvent {
    public BackwardBarrier(MessageDirection flowDirection) {
        super(flowDirection);
    }

    public BackwardBarrier() {
    }
}
