package elements;

import elements.enums.*;
import elements.features.Parts;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import org.apache.flink.streaming.api.operators.graph.OutputTags;

import java.util.List;

/**
 * {@link GraphElement} that are replicable. Added master sync logic
 *
 * @implNote {@link Feature} is a {@link ReplicableGraphElement}
 * but if it is attached to non-replicable {@link GraphElement} it will behave so as well
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
     * If this is REPLICA clearFeatures & if not halo --> send {@link SyncRequest} to Master before running callbacks
     * Since we cannot always assume that Master exists (because of possible lateness),
     * the {@link SyncRequest} should arrive before any other output is triggered in {@link Plugin} to perform dummy element creation
     * </p>
     */
    @Override
    public void create() {
        if (state() == ReplicaState.REPLICA && features != null) features.clear();
        if (!isHalo() && isReplicable() && state() == ReplicaState.REPLICA) {
            GraphOp syncRequestMessage = new GraphOp(Op.SYNC_REQUEST, getMasterPart(), new SyncRequest(this));
            getGraphRuntimeContext().output(syncRequestMessage, OutputTags.ITERATE_OUTPUT_TAG);
        }
        super.create();
    }

    /**
     * {@inheritDoc}
     * Update without replication
     */
    @Override
    public void sync(GraphElement newElement) {
        updateInternal(newElement);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Add newly arrived part number to the <strong>parts</strong> list
     * Only sends back a SYNC if there are non-halo {@link Feature} or if this is a STANDALONE {@link Feature} itself
     * </p>
     */
    @Override
    public void syncRequest(GraphElement newElement) {
        if (!containsFeature("p")) {
            Parts p = new Parts("p", new ShortArrayList(List.of(newElement.getPart())), true);
            p.setElement(this, false);
            p.createInternal();
        } else {
            Rmi.execute(
                    getFeature("p"),
                    "add",
                    newElement.getPart()
            );
        }
        if ((features != null && features.stream().anyMatch(feature -> !feature.isHalo())) || getType() == ElementType.STANDALONE_FEATURE) {
            GraphElement cpy = copy(CopyContext.SYNC);
            getGraphRuntimeContext().output(new GraphOp(Op.SYNC, newElement.getPart(), cpy), OutputTags.ITERATE_OUTPUT_TAG);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @implNote REPLICA element receiving updates will trigger {@link IllegalStateException} since not allowed
     * <p>
     * If this newElement is a Feature or brings some non-halo features with it, before executing update will send SYNC to all replicas
     * </p>
     */
    @Override
    public void update(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) {
            if (!isHalo() && isReplicable() && !getReplicaParts().isEmpty() && (getType() == ElementType.ATTACHED_FEATURE || getType() == ElementType.STANDALONE_FEATURE || (newElement.features != null && newElement.features.stream().anyMatch(feature -> !feature.isHalo())))) {
                GraphOp message = new GraphOp(Op.SYNC, newElement.copy(CopyContext.SYNC));
                getGraphRuntimeContext().broadcast(message, OutputTags.ITERATE_OUTPUT_TAG, getReplicaParts());
            }
            super.update(newElement);
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
    @SuppressWarnings("unchecked")
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
                "master='" + masterPart + '\'' +
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
