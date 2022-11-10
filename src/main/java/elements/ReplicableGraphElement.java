package elements;

import elements.enums.*;
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

    public ReplicableGraphElement() {
        super();
    }

    public ReplicableGraphElement(short master) {
        super();
        this.master = master;
    }

    public ReplicableGraphElement(ReplicableGraphElement element, CopyContext copyContext) {
        super(element, copyContext);
        this.master = element.master;
    }

    @Override
    abstract public ReplicableGraphElement copy(CopyContext context);

    /**
     * Create the graph element. Assigns a parts feature for masters & sends sync request for replicas.
     * First send the sync messages then do the synchronization logic
     */
    @Override
    public void create() {
        if (state() == ReplicaState.REPLICA) clearFeatures(); // Replicas will have features synced back to them, so no need to create
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
            if(!containsFeature("p")) setFeature("p", new Parts(new ArrayList<>(), true));
            Rmi.execute(
                    getFeature("p"),
                    new Rmi(Feature.encodeFeatureId("p", getId(), elementType()), "add", ElementType.ATTACHED_FEATURE, new Object[]{newElement.getPartId()}, true)
            );
            syncReplicas(List.of(newElement.getPartId()));
        } else if (state() == ReplicaState.REPLICA) {
            storage.runCallback(updateElement(newElement, null).f0);
        }
    }

    /**
     * master -> update element, if changed sync message send
     * replica -> Illegal, False Message
     *
     * @param newElement newElement to update with
     */
    @Override
    public void update(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) {
            Tuple2<Consumer<Plugin>, GraphElement> tmp = updateElement(newElement, null);
            if (tmp.f0 != null) syncReplicas(replicaParts());
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
            storage.layerFunction.message(new GraphOp(Op.REMOVE, masterPart(), copy(CopyContext.MEMENTO)), MessageDirection.ITERATE);
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
