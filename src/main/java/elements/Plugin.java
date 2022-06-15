package elements;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.util.ArrayList;
import java.util.List;

/**
 * Plugin is a unique Graph element that is attached to storage, so it is not in the life cycle of logical keys
 */
public class Plugin extends ReplicableGraphElement {
    public transient List<Short> thisReplicaKeys; // Keys(Parts) hashed to this parallel operator Except from master
    public transient List<Short> othersMasterParts; // Master keys hashed to other parallel operators

    public Plugin() {
        super(null, false, (short) 0);
    }

    public Plugin(String id) {
        super(id, false, (short) 0);
    }

    @Override
    public Boolean create() {
        throw new IllegalStateException("Plugins are not created");
    }

    @Override
    public Tuple2<Boolean, GraphElement> update(GraphElement newElement) {
        throw new IllegalStateException("Plugins are not created");
    }

    @Override
    public Tuple2<Boolean, GraphElement> sync(GraphElement newElement) {
        throw new IllegalStateException("Plugins are not created");
    }

    @Override
    public Boolean delete() {
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
        return thisReplicaKeys;
    }

    /**
     * @return parts that are the local master parts of each parallel sub-operators
     */
    public List<Short> othersMasterParts() {
        return othersMasterParts;
    }

    /**
     * Is this key the last one in this operator
     */
    public boolean isLastReplica() {
        return replicaParts().isEmpty() || getPartId() == replicaParts().get(replicaParts().size() - 1);
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
     */
    public void onOperatorEvent(OperatorEvent event) {

    }

    /**
     * Callback when the system closes. Perform all the clean-up
     */
    public void close() {
        // pass
    }

    /**
     * Callback when the system closes. Perform all the initialization
     */
    public void open() {
        setOperatorKeys();
    }

    /**
     * Callback when the plugin is added to the storage for the first time on client side.
     *
     * @implNote Anything initialized here will be serialized and sent to task manager
     */
    public void add() {
        // pass
    }

    // HELPER METHOD

    /**
     * Populates the replicaParts, Master parts and thisReplicaParts for Plugins
     */
    public void setOperatorKeys() {
        try {
            int index = storage.layerFunction.getRuntimeContext().getIndexOfThisSubtask();
            int maxParallelism = storage.layerFunction.getRuntimeContext().getMaxNumberOfParallelSubtasks();
            int parallelism = storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks();
            boolean[] seen = new boolean[parallelism];
            List<Short> thisReplicaKeys = new ArrayList<>(); // Keys of this operator
            List<Short> otherMasterKeys = new ArrayList<>(); // Replica master keys
            for (short i = 0; i < maxParallelism; i++) {
                int operatorIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(String.valueOf(i), maxParallelism, parallelism);
                if (operatorIndex == index) {
                    thisReplicaKeys.add(i);
                } else if (!seen[operatorIndex]) {
                    otherMasterKeys.add(i);
                }
                seen[operatorIndex] = true;
            }

            this.master = thisReplicaKeys.remove(0);
            this.thisReplicaKeys = thisReplicaKeys;
            this.othersMasterParts = otherMasterKeys;
        } catch (Exception e) {
            throw new RuntimeException("Not all parts can be hashed try with different parallelism");
        }
    }

}
