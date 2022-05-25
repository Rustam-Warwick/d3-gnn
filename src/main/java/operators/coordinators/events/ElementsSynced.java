package operators.coordinators.events;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * Called after the first iteration of the watermark
 * Signifies that all elements less that the watermarks are synced in this operator
 */
public class ElementsSynced implements OperatorEvent {
}
