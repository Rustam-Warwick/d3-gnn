package operators.events;

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
}
