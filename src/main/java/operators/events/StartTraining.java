package operators.events;

/**
 * Sent to master from last operator and called on latency marker IDLE
 * Notifies the operators to start the training
 */
public class StartTraining extends FlowingOperatorEvent {

    public StartTraining(Short broadcastCount) {
        super(broadcastCount);
    }

    public StartTraining() {
    }
}
