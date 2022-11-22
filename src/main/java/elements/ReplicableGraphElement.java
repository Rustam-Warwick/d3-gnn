package elements;

import elements.enums.*;
import features.Parts;
import org.apache.flink.api.java.tuple.Tuple2;
import storage.BaseStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * GraphElement that are replicable. Added master sync logic
 */
abstract public class ReplicableGraphElement extends GraphElement {

    /**
     * Master part of this element
     */
    public short masterPart = -1;

    public ReplicableGraphElement() {
        super();
    }

    public ReplicableGraphElement(short masterPart) {
        super();
        this.masterPart = masterPart;
    }

    public ReplicableGraphElement(ReplicableGraphElement element, CopyContext copyContext) {
        super(element, copyContext);
        this.masterPart = element.masterPart;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public ReplicableGraphElement copy(CopyContext context);

    /**
     * {@inheritDoc}
     * If replica send a sync message after creating
     */
    @Override
    public Consumer<BaseStorage> create() {
        if (state() == ReplicaState.REPLICA && features != null) features.clear();
        Consumer<BaseStorage> callback = super.create();
        if (callback != null && state() == ReplicaState.REPLICA && !isHalo()) {
            SyncElement syncElement = new SyncElement(this);
            getStorage().layerFunction.message(new GraphOp(Op.SYNC_REQUEST, getMasterPart(), syncElement), MessageDirection.ITERATE);
        }
        return callback;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Master only sends back if there are non-halo {@link Feature} or if STANDALONE {@link Feature}
     * </p>
     */
    @Override
    public void sync(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) {
            if (!containsFeature("p")) {
                Parts p = new Parts("p", new ArrayList<>(2), true, (short) -1);
                p.setElement(this, false);
                p.createInternal(); // Don't bother with callbacks
            }
            Rmi.execute(
                    getFeature("p"),
                    "add",
                    newElement.getPart()
            );
            GraphElement cpy = copy(CopyContext.SYNC);
            if (cpy.features != null || cpy.getType() == ElementType.STANDALONE_FEATURE)
                getStorage().layerFunction.message(new GraphOp(Op.SYNC, newElement.getPart(), cpy), MessageDirection.ITERATE);
        } else if (state() == ReplicaState.REPLICA) {
            getStorage().runCallback(super.update(newElement).f0);
        }
    }

    /**
     * {@inheritDoc}
     * master -> triggerUpdate element, if changed sync message send
     * replica -> @throw {@link IllegalStateException}
     */
    @Override
    public Tuple2<Consumer<BaseStorage>, GraphElement> update(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) {
            Tuple2<Consumer<BaseStorage>, GraphElement> tmp = super.update(newElement);
            if (tmp.f0 != null && !isHalo() && !getReplicaParts().isEmpty()) {
                ReplicableGraphElement cpy = copy(CopyContext.SYNC);
                getReplicaParts().forEach(part_id -> getStorage().layerFunction.message(new GraphOp(Op.SYNC, part_id, cpy), MessageDirection.ITERATE));
            }
            return tmp;
        } else {
            throw new IllegalStateException("REPLICAS Should not received Updates");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short getMasterPart() {
        return this.masterPart;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Short> getReplicaParts() {
        if (!containsFeature("p")) return super.getReplicaParts();
        return (List<Short>) (getFeature("p")).getValue(); // @implNote Never create other Feature with the name parts
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getType() + "{" +
                "id='" + getId() + '\'' +
                '}';
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReplicable() {
        return true;
    }
}
