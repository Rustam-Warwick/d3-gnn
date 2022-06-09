package operators.events;

import java.util.Objects;

public class WatermarkEvent extends IterableOperatorEvent {
    public long timestamp;
    public WatermarkEvent(long timestamp){
        this.timestamp = timestamp;
    }
    public WatermarkEvent(long timestamp,Short currentIteration) {
        super(currentIteration);
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        WatermarkEvent that = (WatermarkEvent) o;
        return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), timestamp);
    }
}
