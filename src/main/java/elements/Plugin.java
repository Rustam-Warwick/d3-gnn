package elements;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.watermark.Watermark;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Plugin is a unique Graph element that is attached to storage, so it is not in the life cycle of logical keys
 */
public class Plugin extends ReplicableGraphElement {
    public transient List<Short> thisReplicaKeys; // Keys(Parts) Hased to this parallel operator
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
     * @return Replica Parts is the parts where else is this plugin replicate apart from its local master part
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

    @Override
    public ElementType elementType() {
        return ElementType.PLUGIN;
    }


    public void addElementCallback(GraphElement element) {
        // pass
    }

    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        // pass
    }

    public void deleteElementCallback(GraphElement deletedElement) {
        // pass
    }

    public void onTimer(long timestamp) {
        // passs
    }

    public void onWatermark(Watermark w) {
        // pass
    }

    public void close() {
        // pass
    }

    public void open() {
        setOperatorKeys();
    }

    public void add() {
        // pass
    }

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
