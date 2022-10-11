package operators.events;

import elements.iterations.MessageDirection;

public class ForwardBarrier extends BaseOperatorEvent {
    public ForwardBarrier(MessageDirection flowDirection) {
        super(flowDirection);
    }

    public ForwardBarrier() {

    }
}
