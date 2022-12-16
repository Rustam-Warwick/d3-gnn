package operators.events;

import elements.enums.MessageDirection;

/**
 * Sent to master from last operator and called on latency marker IDLE
 * Notifies the operators to startTermination the training
 */
public class StartTraining extends BaseOperatorEvent {
    public StartTraining(MessageDirection flowDirection) {
        super(flowDirection);
    }

    public StartTraining() {
    }
}
