package operators.events;

public class InferenceBarrier extends FlowingOperatorEvent {
    public InferenceBarrier(Short broadcastCount) {
        super(broadcastCount);
    }

    public InferenceBarrier() {
    }
}
