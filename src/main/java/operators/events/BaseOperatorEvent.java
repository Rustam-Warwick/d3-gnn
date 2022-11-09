package operators.events;

import elements.enums.MessageDirection;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

/**
 * Parent class for OperatorEvents that flow though the GNN pipeline and interact with storage layers and plugins
 * Event flowing though pipeline is only triggered when received by all sending parties
 */
public class BaseOperatorEvent implements OperatorEvent {
    public MessageDirection direction;

    public BaseOperatorEvent(MessageDirection flowDirection) {
        this.direction = flowDirection;
    }

    public BaseOperatorEvent() {

    }

    public MessageDirection getDirection() {
        return direction;
    }

    public BaseOperatorEvent setDirection(MessageDirection direction) {
        this.direction = direction;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseOperatorEvent that = (BaseOperatorEvent) o;
        return Objects.equals(direction, that.direction);
    }

    @Override
    public String toString() {
        return "BaseOperatorEvent{" +
                "direction=" + direction +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), direction);
    }
}
