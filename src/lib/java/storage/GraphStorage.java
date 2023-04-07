package storage;

import org.apache.flink.runtime.state.tmshared.TMSharedKeyedStateBackend;
import org.apache.flink.runtime.state.tmshared.TMSharedState;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base Class for all Graph Storage States
 * <p>
 * Each graph state lives in the TM local state
 * To facilitate faster access across various shared operators a {@link GraphView} should be created holding the {@link GraphRuntimeContext}
 * </p>
 */
abstract public class GraphStorage extends TMSharedState {

    protected static final Logger LOG = LoggerFactory.getLogger(GraphStorage.class);

    /**
     * Will fail if the storage object is created outside of {@link GraphRuntimeContext}
     */
    @Override
    public void register(TMSharedKeyedStateBackend<?> TMSharedKeyedStateBackend) {
        Preconditions.checkNotNull(GraphRuntimeContext.CONTEXT_THREAD_LOCAL.get(), "Graph Storage can only be used in GraphStorage Operators. GraphRuntimeContext is not detected");
        super.register(TMSharedKeyedStateBackend);
    }

    /**
     * Retrieve the {@link GraphView} to future access
     */
    abstract public GraphView getGraphStorageView(GraphRuntimeContext runtimeContext);

}
