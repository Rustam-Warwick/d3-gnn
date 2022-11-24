package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import operators.events.BaseOperatorEvent;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import storage.BaseStorage;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Plugin is a unique {@link GraphElement} that is attached to storage;
 * <p>
 * It always lives in the <strong>Operator State</strong> so it is not in the life-cycle of the logical keys
 * Furthermore, Plugins cam be thought of as extension of Storage, as they are allowed to created their own <strong>KeyedStates</strong>
 * </p>
 */
@SuppressWarnings("unused")
public class Plugin extends ReplicableGraphElement implements CheckpointedFunction {

    /**
     * ID of this plugin, should be unique per storage
     */
    final public String id;

    public Plugin() {
        super((short) 0);
        id = null;
    }

    public Plugin(String id) {
        super((short) 0);
        this.id = id;
    }

    /**
     * {@link IllegalStateException}
     */
    @Override
    public ReplicableGraphElement copy(CopyContext context) {
        throw new IllegalStateException("No copy");
    }

    /**
     * {@link IllegalStateException}
     */
    @Override
    public Consumer<BaseStorage> create() {
        throw new IllegalStateException("Plugins are not created");
    }

    /**
     * {@link IllegalStateException}
     */
    @Override
    public Consumer<BaseStorage> update(GraphElement newElement) {
        throw new IllegalStateException("Plugins are not updated");
    }

    /**
     * {@link IllegalStateException}
     */
    @Override
    public void sync(GraphElement newElement) {
        throw new IllegalStateException("Plugins are not synced");
    }

    /**
     * {@link IllegalStateException}
     */
    @Override
    public Consumer<BaseStorage> delete() {
        throw new IllegalStateException("Plugins are not deleted");
    }

    /**
     * @return thisOperatorParts()
     */
    @Override
    public List<Short> getReplicaParts() {
        return getStorage().layerFunction.getWrapperContext().getThisOperatorParts();
    }

    /**
     * @return parts that are the local master parts of each parallel sub-operators
     */
    public List<Short> othersMasterParts() {
        return getStorage().layerFunction.getWrapperContext().getOtherOperatorMasterParts();
    }

    /**
     * Is this key the last one in this operator
     */
    public boolean isLastReplica() {
        return getReplicaParts().isEmpty() || Objects.equals(getPart(), getReplicaParts().get(getReplicaParts().size() - 1));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return ElementType.PLUGIN;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return id;
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
