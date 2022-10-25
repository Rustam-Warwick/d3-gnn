package elements;

import elements.iterations.MessageDirection;
import elements.iterations.RemoteFunction;
import elements.iterations.RemoteInvoke;
import features.Set;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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


    // CRUD Methods

    /**
     * Create the graph element. Assigns a parts feature for masters & sends sync request for replicas.
     * First send the sync messages then do the synchronization logic
     */
    @Override
    public void create() {
        assert storage != null;
        if (state() == ReplicaState.REPLICA) clearFeatures(); // Replicas will have features synced back to them
        Consumer<Plugin> callback = createElement();
        if (callback != null && state() == ReplicaState.REPLICA && !isHalo())
            storage.layerFunction.message(new GraphOp(Op.SYNC, masterPart(), this), MessageDirection.ITERATE);
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
                setFeature("p", new Set<Short>(new ArrayList<>(), true)); // Lazy part list creation
            new RemoteInvoke()
                    .toElement(Feature.encodeAttachedFeatureId("p", getId(), elementType()), ElementType.FEATURE)
                    .hasUpdate()
                    .method("add")
                    .addDestination(masterPart())
                    .where(MessageDirection.ITERATE)
                    .withArgs(newElement.getPartId())
                    .buildAndRun(storage);
            syncReplicas(List.of(newElement.getPartId()));
        } else if (state() == ReplicaState.REPLICA) {
            super.update(newElement);
        }
    }

    /**
     * master -> update element, if changed send message to replica
     * replica -> Redirect to master, false message
     *
     * @param newElement newElement to update with
     */
    @Override
    public void update(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) {
            assert storage != null;
            Tuple2<Consumer<Plugin>, GraphElement> tmp = updateElement(newElement, null);
            if (tmp.f0 != null && !isHalo()) syncReplicas(replicaParts());
            storage.runCallback(tmp.f0);
        } else {
            throw new IllegalStateException("No one should receive updates other than MASTER");
        }
    }

    /**
     * master -> Send delete message to replica, actually delete the element from master immediately
     * replica -> Redirect this message to master, replica deletions are happening through RMI deleteReplica
     */
    @Override
    public void delete() {
        if (state() == ReplicaState.MASTER) {
            new RemoteInvoke()
                    .toElement(getId(), elementType())
                    .noUpdate()
                    .method("deleteReplica")
                    .addDestinations(replicaParts())
                    .where(MessageDirection.ITERATE)
                    .withArgs(false)
                    .buildAndRun(storage);
            super.delete();
        } else if (state() == ReplicaState.REPLICA) {
            assert storage != null;
            storage.layerFunction.message(new GraphOp(Op.REMOVE, masterPart(), copy()), MessageDirection.ITERATE);
        }
    }

    /**
     * Deletes a replica directly from storage, if notifyMaster also removes it from the parts
     * @param notifyMaster should notify it master part after deletion?
     */
    @RemoteFunction
    public void deleteReplica(boolean notifyMaster) {
        if (this.state() == ReplicaState.REPLICA) {
            super.delete();
            if (notifyMaster) new RemoteInvoke()
                    .toElement(Feature.encodeAttachedFeatureId("p", getId(), elementType()), ElementType.FEATURE)
                    .hasUpdate()
                    .method("remove")
                    .withArgs(getPartId())
                    .where(MessageDirection.ITERATE)
                    .addDestination(masterPart())
                    .buildAndRun(storage);
        }
    }



    // NORMAL OPERATIONS

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
        return (List<Short>) Objects.requireNonNull(getFeature("p")).getValue(); // @implNote Never create other Feature with the name parts
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReplicable() {
        return true;
    }
}
