package operators.events;

import elements.enums.MessageDirection;

public class ForwardBarrier extends BaseOperatorEvent {
    public ForwardBarrier(MessageDirection flowDirection) {
        super(flowDirection);
    }

    public ForwardBarrier() {

    }
}
