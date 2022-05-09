package elements;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.util.ArrayList;
import java.util.List;

/**
 * Plugin is a unique Graph element that is attached to storage, so it is not in the life cycle of logical keys
 */
public class Plugin extends ReplicableGraphElement {
    public transient List<Short> thisReplicaKeys; // Keys(Parts) hashed to this parallel operator
    public transient List<Short> othersMasterParts; // Master keys hashed to other parallel operators

    public Plugin() {
        super(null, false, (short) 0);
    }

    public Plugin(String id) {
        super(id, false, (short) 0);
    }

    @Override
    public Boolean create() {
        return false;
    }

    @Override
    public Tuple2<Boolean, GraphElement> update(GraphElement newElement) {
        return null;
    }

    @Override
    public Tuple2<Boolean, GraphElement> sync(GraphElement newElement) {
        return null;
    }

    @Override
    public Boolean delete() {
        return false;
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
     * @return Element Type
     */
    @Override
    public ElementType elementType() {
        return ElementType.PLUGIN;
    }

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
     * Callback when the system receives a proper watermark. Aligned with all the replica events
     * Will be called one-by-one for each part that is in this operator
     * Order will be exactly as [masterPart(), ...replicaParts()]
     * Watermarks are encoded as such:
     * (wt % 4) is the watermark iteration number. Only iteration 3 watermarks are actually committed and pushed to the next layer
     *
     * @param timestamp timestamp of the watermark
     */
    public void onWatermark(long timestamp) {
        // pass
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
            master = thisReplicaKeys.remove(0);
            this.thisReplicaKeys = thisReplicaKeys;
            this.othersMasterParts = otherMasterKeys;
        } catch (Exception e) {
            throw new RuntimeException("Not all parts can be hashed try with different parallelism");
        }
    }

}
