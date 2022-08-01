package operators.events;

import java.util.Objects;

/**
 * Operator Events that can also flow in the pipeline through broadcast calls
 * If this event is flowing need to also set the broadcastCount, as we need to know how many of such events are expected to arrive
 */
public class FlowingOperatorEvent extends BaseOperatorEvent {
    public Short broadcastCount;

    public FlowingOperatorEvent(Short broadcastCount) {
        this.broadcastCount = broadcastCount;
    }

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
        return Objects.hash(getClass(), broadcastCount);
    }
}
