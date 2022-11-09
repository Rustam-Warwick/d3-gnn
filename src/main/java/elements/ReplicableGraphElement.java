package elements;

import elements.enums.ElementType;
import elements.enums.Op;
import elements.enums.ReplicaState;
import elements.enums.MessageDirection;
import elements.annotations.RemoteFunction;
import features.Parts;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * GraphElement that are replicable. Added halo and master logic
 */
abstract public class ReplicableGraphElement extends GraphElement {

    public short master = -1;

    public boolean halo = false;

    public ReplicableGraphElement() {
        super();
    }

    public ReplicableGraphElement(boolean halo, short master) {
        super();
        this.halo = halo;
        this.master = master;
    }

    public ReplicableGraphElement(ReplicableGraphElement element, boolean deepCopy) {
        super(element, deepCopy);
        this.master = element.master;
        this.halo = element.halo;
    }

    @Override
    abstract public ReplicableGraphElement copy();

    @Override
    abstract public ReplicableGraphElement deepCopy();

    /**
     * Create the graph element. Assigns a parts feature for masters & sends sync request for replicas.
     * First send the sync messages then do the synchronization logic
     */
    @Override
    public void create() {
        // assert storage != null;
        if (state() == ReplicaState.REPLICA) clearFeatures(); // Replicas will have features synced back to them
        Consumer<Plugin> callback = createElement();
        if (callback != null && state() == ReplicaState.REPLICA && !isHalo()) {
            SyncElement syncElement = new SyncElement(getId(), elementType());syncElement.setStorage(storage);
            storage.layerFunction.message(new GraphOp(Op.SYNC, masterPart(), syncElement), MessageDirection.ITERATE);
        }
        storage.runCallback(callback);
    }

    /**
     * Master -> Add to part feature, send syncReplicas request
     * Replica -> Accept the sync and update the element
     *
     * @param newElement New element to sync with
     */
    @Override
    public void sync(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) {
            assert newElement.getPartId() != -1;
            if (!containsFeature("p"))
                setFeature("p", new Parts(new ArrayList<>(), true)); // Lazy part list creation
            Rmi.buildAndRun(
                    new Rmi(Feature.encodeFeatureId("p", getId(), elementType()), "add", ElementType.ATTACHED_FEATURE, new Object[]{newElement.getPartId()}, true), storage,
                    masterPart(),
                    MessageDirection.ITERATE
            );
            syncReplicas(List.of(newElement.getPartId()));
        } else if (state() == ReplicaState.REPLICA) {
            storage.runCallback(updateElement(newElement, null).f0);
        }
    }

    /**
     * master -> update element, if changed send message to replica
     * replica -> Illegal, False Message
     *
     * @param newElement newElement to update with
     */
    @Override
    public void update(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) {
            // assert storage != null;
            Tuple2<Consumer<Plugin>, GraphElement> tmp = updateElement(newElement, null);
            if (tmp.f0 != null && !isHalo()) syncReplicas(replicaParts());
            storage.runCallback(tmp.f0);
        } else {
            throw new IllegalStateException("REPLICAS Should not received Updates");
        }
    }

    /**
     * master -> Send delete message to replica, actually delete the element from master immediately
     * replica -> Redirect this message to master, replica deletions are happening through RMI deleteReplica
     */
    @Override
    public void delete() {
        if (state() == ReplicaState.MASTER) {
            Rmi.buildAndRun(
                    new Rmi(getId(), "deleteReplica", elementType(), new Object[]{false}, true),
                    storage,
                    replicaParts(),
                    MessageDirection.ITERATE
            );
            super.delete();
        } else if (state() == ReplicaState.REPLICA) {
            // assert storage != null;
            storage.layerFunction.message(new GraphOp(Op.REMOVE, masterPart(), copy()), MessageDirection.ITERATE);
        }
    }

    /**
     * Deletes a replica directly from storage, if notifyMaster also removes it from the parts
     *
     * @param notifyMaster should notify it master part after deletion?
     */
    @RemoteFunction
    public void deleteReplica(boolean notifyMaster) {
        if (this.state() == ReplicaState.REPLICA) {
            super.delete();
            if (notifyMaster)
                Rmi.buildAndRun(
                        new Rmi(Feature.encodeFeatureId("p", getId(), elementType()), "remove", ElementType.ATTACHED_FEATURE, new Object[]{getPartId()}, true), storage,
                        masterPart(),
                        MessageDirection.ITERATE
                );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short masterPart() {
        return this.master;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isHalo() {
        return this.halo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Short> replicaParts() {
        if (!containsFeature("p")) return super.replicaParts();
        return (List<Short>) (getFeature("p")).getValue(); // @implNote Never create other Feature with the name parts
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReplicable() {
        return true;
    }
}
