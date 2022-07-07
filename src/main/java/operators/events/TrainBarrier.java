package operators.events;

public class TrainBarrier extends FlowingOperatorEvent{
    public TrainBarrier(Short broadcastCount) {
        super(broadcastCount);
    }

    public TrainBarrier() {
    }
}
