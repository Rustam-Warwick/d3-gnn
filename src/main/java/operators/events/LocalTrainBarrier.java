package operators.events;

import elements.iterations.MessageDirection;

public class LocalTrainBarrier extends BaseOperatorEvent {
    public LocalTrainBarrier(MessageDirection flowDirection) {
        super(flowDirection);
    }

    public LocalTrainBarrier() {
    }
}
