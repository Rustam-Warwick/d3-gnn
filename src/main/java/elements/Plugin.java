package elements;

import operators.events.BaseOperatorEvent;

import java.util.List;
import java.util.Objects;

/**
 * Plugin is a unique Graph element that is attached to storage, so it is not in the life cycle of logical keys
 */
public class Plugin extends ReplicableGraphElement {
    public Plugin() {
        super(null, false, (short) 0);
    }

    public Plugin(String id) {
        super(id, false, (short) 0);
    }

    @Override
    public void create() {
        throw new IllegalStateException("Plugins are not created");
    }

    @Override
    public void update(GraphElement newElement) {
        throw new IllegalStateException("Plugins are not created");
    }

    @Override
    public void sync(GraphElement newElement) {
        throw new IllegalStateException("Plugins are not created");
    }

    @Override
    public short getPartId() {
        return storage.layerFunction.getCurrentPart();
    }

    @Override
    public void setPartId(short partId) {
        // Do not set instead fetch from the stroage
    }

    @Override
    public void delete() {
        throw new IllegalStateException("Plugins are not created");
    }

    @Override
    public ReplicableGraphElement copy() {
        throw new IllegalStateException("No copy");
    }

    @Override
    public ReplicableGraphElement deepCopy() {
        throw new IllegalStateException("No deepcopy");
    }

    /**
     * @return Replica Parts are the parts where else is this plugin replicate apart from its local master part
     */
    @Override
    public List<Short> replicaParts() {
        return storage.layerFunction.getWrapperContext().getThisOperatorParts();
    }

    /**
     * @return parts that are the local master parts of each parallel sub-operators
     */
    public List<Short> othersMasterParts() {
        return storage.layerFunction.getWrapperContext().getOtherOperatorMasterParts();
    }

    /**
     * Is this key the last one in this operator
     */
    public boolean isLastReplica() {
        return replicaParts().isEmpty() || Objects.equals(getPartId(), replicaParts().get(replicaParts().size() - 1));
    }

    /**
     * @return Element Type
     */
    @Override
    public ElementType elementType() {
        return ElementType.PLUGIN;
    }


    // GraphElement Callbacks

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

    // CALLBACKS

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
     *
     * @param event
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
     * Callback when the plugin is added to the storage for the first time on client side.
     *
     * @implNote Anything initialized here will be serialized and sent to task manager
     */
    public void add() {
        // pass
    }


}
