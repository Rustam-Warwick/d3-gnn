package operators.events;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

public class IterableOperatorEvent implements OperatorEvent {
    public Short currentIteration;


    public IterableOperatorEvent(Short currentIteration) {
        this.currentIteration = currentIteration;
    }

    public Short getCurrentIteration() {
        return currentIteration;
    }

    public void setCurrentIteration(Short currentIteration) {
        this.currentIteration = currentIteration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IterableOperatorEvent that = (IterableOperatorEvent) o;
        return Objects.equals(currentIteration, that.currentIteration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentIteration);
    }
}
