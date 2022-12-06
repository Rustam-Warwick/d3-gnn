package plugins;

import elements.GraphElement;
import operators.GraphProcessContext;
import operators.events.BaseOperatorEvent;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.BaseStorage;

/**
 * <p>
 * It always lives in the <strong>Operator State</strong> so it is not in the life-cycle of the logical keys
 * Furthermore, Plugins cam be thought of as extension of Storage, as they are allowed to created their own <strong>KeyedStates</strong>
 * </p>
 */
@SuppressWarnings("unused")
public class Plugin implements CheckpointedFunction {

    /**
     * Plugin Logger
     */
    protected final static Logger LOG = LoggerFactory.getLogger(Plugin.class);

    /**
     * ID of this plugin, should be unique per storage
     */
    final public String id;

    /**
     * Is this Plugin Active
     */
    public boolean IS_ACTIVE = true;

    public Plugin() { id= null;}

    public Plugin(String id) {
        this.id = id;
    }

    public Plugin(String id, boolean IS_ACTIVE) {
        this.id = id;
        this.IS_ACTIVE = IS_ACTIVE;
    }


    public String getId() {
        return id;
    }

    /**
     * Stop this plugin
     */
    public void stop() {
        IS_ACTIVE = false;
    }

    /**
     * Start this plugin
     */
    public void start() {
        IS_ACTIVE = true;
    }

    public BaseStorage getStorage(){
        return GraphProcessContext.getContext().getStorage();
    }

    // ----------------------- CALLBACKS --------------------

    /**
     * Callback when a graph element is created
     */
    public void addElementCallback(GraphElement element) {
        // pass
    }

    /**
     * Callback when a graph element is updated
     */
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        // pass
    }

    /**
     * Callback when a graph element is removed
     */
    public void deleteElementCallback(GraphElement deletedElement) {
        // pass
    }

    /**
     * Callback when the timer fires on {@link BaseStorage}
     */
    public void onTimer(long timestamp) {
        // pass
    }

    /**
     * Callback when Operator sends event to this plugin
     */
    public void onOperatorEvent(BaseOperatorEvent event) {
        // pass
    }

    /**
     * Callback when the {@link BaseStorage} closes. Perform all the clean-up
     */
    public void close() throws Exception {
        // pass
    }

    /**
     * Callback when the system closes. Perform all the initialization
     */
    public void open() throws Exception {
        // pass
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("RedundantThrows")
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("RedundantThrows")
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Pass
    }
}
