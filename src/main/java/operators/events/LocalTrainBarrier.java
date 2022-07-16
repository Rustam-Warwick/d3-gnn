package operators.events;

public class LocalTrainBarrier extends FlowingOperatorEvent {
    public LocalTrainBarrier(Short broadcastCount) {
        super(broadcastCount);
    }

    public LocalTrainBarrier() {
    }
}
