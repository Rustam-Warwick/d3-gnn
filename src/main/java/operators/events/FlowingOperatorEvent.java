package operators.events;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

/**
 * Operator Events that can also flow in the pipeline through broadcast calls
 */
public class FlowingOperatorEvent implements OperatorEvent {
    public Short broadcastCount;

    public FlowingOperatorEvent() {

    }

    public Short getBroadcastCount() {
        return broadcastCount;
    }

    public void setBroadcastCount(Short broadcastCount) {
        this.broadcastCount = broadcastCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowingOperatorEvent that = (FlowingOperatorEvent) o;
        return Objects.equals(broadcastCount, that.broadcastCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(),broadcastCount);
    }
}
