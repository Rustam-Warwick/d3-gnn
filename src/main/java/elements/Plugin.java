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
 * Plugin is a unique Graph element that is attached to storage, so it is not in the life cycle of logical keys
 */
public class Plugin extends ReplicableGraphElement implements CheckpointedFunction {

    /**
     * Id of this plugin, should be unique per storage
     */
    public String id;

    public Plugin() {
        super((short) 0);
    }

    public Plugin(String id) {
        super((short) 0);
        this.id = id;
    }

    @Override
    public ReplicableGraphElement copy(CopyContext context) {
        throw new IllegalStateException("No copy");
    }

    @Override
    public Consumer<BaseStorage> create() {
        throw new IllegalStateException("Plugins are not created");
    }

    @Override
    public Consumer<BaseStorage> update(GraphElement newElement) {
        throw new IllegalStateException("Plugins are not updated");
    }

    @Override
    public void sync(GraphElement newElement) {
        throw new IllegalStateException("Plugins are not synced");
    }

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
     * @return Element Type
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
     *
     * @param element Newly created GraphElement
     */
    public void addElementCallback(GraphElement element) {
        // pass
    }

    /**
     * Callback when a graph element is updated
     *
     * @param newElement newElement commited to memory
     * @param oldElement oldElement removed from memory
     */
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        // pass
    }

    /**
     * Callback when a graph element is removed
     *
     * @param deletedElement element removed from memory
     */
    public void deleteElementCallback(GraphElement deletedElement) {
        // pass
    }

    /**
     * Callback when the timer fires
     *
     * @param timestamp firing timestamp
     */
    public void onTimer(long timestamp) {
        // passs
    }

    /**
     * Callback when OperatorSends event to this plugin
     */
    public void onOperatorEvent(BaseOperatorEvent event) {
        // pass
    }

    /**
     * Callback when the system closes. Perform all the clean-up
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
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Pass
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Pass
    }
}
