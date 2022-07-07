package operators.events;

import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

public class WatermarkStatusEvent extends FlowingOperatorEvent {
    public int status;

    public WatermarkStatusEvent(int status, Byte currentIteration) {
        super();
        this.status = status;
    }

    public WatermarkStatus getWatermarkStatus() {
        return new WatermarkStatus(this.status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        WatermarkStatusEvent that = (WatermarkStatusEvent) o;

        return status == that.status;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + status;
        return result;
    }
}
