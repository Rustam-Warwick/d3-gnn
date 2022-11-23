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
     * <p>
     *     If this is REPLICA clearFeatures & if not halo; send {@link SyncElement} to Master before running callbacks
     *     Since we cannot always assume that Master exists (because of possible lateness), the {@link SyncElement} should arrive before any other message in triggered in {@link Plugin}
     * </p>
     */
    @Override
    public Consumer<BaseStorage> create() {
        if (state() == ReplicaState.REPLICA && features != null) features.clear();
        Consumer<BaseStorage> callback = super.create();
        if (state() == ReplicaState.REPLICA && !isHalo()) {
            SyncElement syncElement = new SyncElement(this);
            return ((Consumer<BaseStorage>) storage -> storage.layerFunction.message(new GraphOp(Op.SYNC_REQUEST, getMasterPart(), syncElement), MessageDirection.ITERATE))
                    .andThen(callback);
        }
        return callback;
    }

    /**
     * {@inheritDoc}
     * Just updateInternal without replicating again.
     */
    @Override
    public void sync(GraphElement newElement) {
       getStorage().runCallback(super.updateInternal(newElement));
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Add newly arrived part number to the <strong>parts</strong> list
     *     Only sends back a SYNC if there are non-halo {@link Feature} or if this is a STANDALONE {@link Feature}
     * </p>
     */
    @Override
    public void syncRequest(GraphElement newElement) {
        if (!containsFeature("p")) {
            Parts p = new Parts("p", new ArrayList<>(List.of(newElement.getPart())), true, (short) -1);
            p.setElement(this, false);
            getStorage().runCallback(p.createInternal());
        }else{
            Rmi.execute(
                    getFeature("p"),
                    "add",
                    newElement.getPart()
            );
        }
        GraphElement cpy = copy(CopyContext.SYNC);
        if (cpy.features != null || cpy.getType() == ElementType.STANDALONE_FEATURE)
            getStorage().layerFunction.message(new GraphOp(Op.SYNC, newElement.getPart(), cpy), MessageDirection.ITERATE);
    }

    /**
     * {@inheritDoc}
     * @implNote REPLICA element receiving updates will trigger {@link IllegalStateException}
     * <p>
     *      After all the update took place will trigger SYNC with all its Replica parts.
     *      Here the {@link Plugin} callbacks will come before the SYNC requests, which is usually okay
     * </p>
     */
    @Override
    public Consumer<BaseStorage> update(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) {
            Consumer<BaseStorage> callback = super.update(newElement);
            if (!isHalo() && !getReplicaParts().isEmpty()) {
                return callback.andThen(storage -> {
                    ReplicableGraphElement cpy = copy(CopyContext.SYNC);
                    getReplicaParts().forEach(part_id -> storage.layerFunction.message(new GraphOp(Op.SYNC, part_id, cpy), MessageDirection.ITERATE));
                });
            }
            return callback;
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
