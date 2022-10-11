package operators.events;

import elements.iterations.MessageDirection;

public class BackwardBarrier extends BaseOperatorEvent {
    public BackwardBarrier(MessageDirection flowDirection) {
        super(flowDirection);
    }

    public BackwardBarrier() {
    }
}
