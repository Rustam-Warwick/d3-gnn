package operators.interfaces;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.Triggerable;

public interface RichGraphElement extends CheckpointedFunction, RichFunction, OperatorEventHandler, Triggerable<PartNumber, VoidNamespace> {

    @Override
    default void open(Configuration parameters) throws Exception {

    }

    @Override
    default void close() throws Exception {

    }

    @Override
    default GraphRuntimeContext getRuntimeContext() {
        return GraphRuntimeContext.getContext();
    }

    @Override
    default IterationRuntimeContext getIterationRuntimeContext() {
        return null;
    }

    @Override
    default void setRuntimeContext(RuntimeContext t) {

    }

    @Override
    default void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    default void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    default void handleOperatorEvent(OperatorEvent evt) {

    }

    @Override
    default void onEventTime(InternalTimer<PartNumber, VoidNamespace> timer) throws Exception {

    }

    @Override
    default void onProcessingTime(InternalTimer<PartNumber, VoidNamespace> timer) throws Exception {

    }
}
