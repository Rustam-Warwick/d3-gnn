package operators.events;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

public class IterableOperatorEvent implements OperatorEvent {
    public Byte currentIteration = 0;

    public IterableOperatorEvent(Byte currentIteration) {
        this.currentIteration = currentIteration;
    }

    public IterableOperatorEvent() {

    }

    public Byte getCurrentIteration() {
        return currentIteration;
    }

    public void setCurrentIteration(Byte currentIteration) {
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
