package operators.events;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * Is sent to the GNN Layer on the second watermark iteration before emitting it to the next operator
 * Used so that plugins can send their messages before the watermark is emitted
 */
public class ActionTaken implements OperatorEvent {
}
