package operators.events;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public class IterableOperatorEvent implements OperatorEvent {
    public Short currentIteration;

    public IterableOperatorEvent() {
    }

    public IterableOperatorEvent(Short currentIteration) {
        this.currentIteration = currentIteration;
    }

    public Short getCurrentIteration() {
        return currentIteration;
    }

    public void setCurrentIteration(Short currentIteration) {
        this.currentIteration = currentIteration;
    }
}
