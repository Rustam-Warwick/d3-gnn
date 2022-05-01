package elements;

import features.Set;
import iterations.MessageDirection;
import iterations.RemoteFunction;
import iterations.RemoteInvoke;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ReplicableGraphElement extends GraphElement {
    public short master;
    public boolean halo;

    public ReplicableGraphElement() {
        this(null, false, (short) -1);
    }

    public ReplicableGraphElement(String id) {
        this(id, false, (short) -1);
    }

    public ReplicableGraphElement(String id, boolean halo) {
        this(id, halo, (short) -1);
    }

    public ReplicableGraphElement(String id, boolean halo, short master) {
        super(id);
        this.halo = halo;
        this.master = master;
    }

    @Override
    public ReplicableGraphElement copy() {
        ReplicableGraphElement tmp = new ReplicableGraphElement(this.id, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public ReplicableGraphElement deepCopy() {
        ReplicableGraphElement tmp = new ReplicableGraphElement(this.id, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    /**
     * Create the graph element. Assigns a parts feature for masters & sends sync request for replicas
     *
     * @return is_created
     */
    @Override
    public Boolean create() {
        if (state() == ReplicaState.REPLICA) features.clear();
        boolean is_created = createElement();
        if (is_created) {
            if (this.state() == ReplicaState.MASTER) {
                // Add setFeature
                this.setFeature("parts", new Set<Short>(new ArrayList<>(), true));
            } else {
                // Send Query
                this.storage.layerFunction.message(new GraphOp(Op.SYNC, this.masterPart(), this, MessageDirection.ITERATE, getTimestamp()));
            }
        }
        return is_created;
    }

    /**
     * Master -> Add to parts feature, send syncReplicas request
     * Replica -> Accept the sync and update the element
     *
     * @param newElement New element to sync with
     * @return (isSynced, oldElement)
     */
    @Override
    public Tuple2<Boolean, GraphElement> sync(GraphElement newElement) {
        if (this.state() == ReplicaState.MASTER) {
            new RemoteInvoke()
                    .toElement(decodeFeatureId("parts"), ElementType.FEATURE)
                    .hasUpdate()
                    .method("add")
                    .addDestination(masterPart())
                    .where(MessageDirection.ITERATE)
                    .withArgs(newElement.getPartId())
                    .withTimestamp(getTimestamp())
                    .buildAndRun(storage);
            syncReplicas(List.of(newElement.getPartId()));
        } else if (this.state() == ReplicaState.REPLICA) {
            return updateElement(newElement);
        }

        return super.sync(this); // Do nothing
    }

    /**
     * master -> update element, if changed send message to replica
     * replica -> Redirect to master, false message
     *
     * @param newElement newElement to update with
     * @return (isUpdated, oldElement)
     */
    @Override
    public Tuple2<Boolean, GraphElement> update(GraphElement newElement) {
        if (this.state() == ReplicaState.MASTER) {
            Tuple2<Boolean, GraphElement> tmp = updateElement(newElement);
            if (tmp._1) this.syncReplicas(replicaParts());
            return tmp;
        } else if (this.state() == ReplicaState.REPLICA) {
            this.storage.layerFunction.message(new GraphOp(Op.COMMIT, this.masterPart(), newElement, MessageDirection.ITERATE, getTimestamp()));
            return new Tuple2<>(false, this);
        } else return super.update(newElement);
    }


    /**
     * master -> Send delete message to replica, actually delete the element from master immediately
     * replica -> Redirect this message to master, replica deletions are happening through RMI deleteReplica
     *
     * @return isDeleted
     */
    @Override
    public Boolean delete() {
        if (state() == ReplicaState.MASTER) {
            new RemoteInvoke()
                    .toElement(getId(), elementType())
                    .noUpdate()
                    .method("deleteReplica")
                    .addDestinations(replicaParts())
                    .where(MessageDirection.ITERATE)
                    .withArgs(false)
                    .withTimestamp(getTimestamp())
                    .buildAndRun(storage);
            return deleteElement();

        } else if (state() == ReplicaState.REPLICA) {
            this.storage.layerFunction.message(new GraphOp(Op.REMOVE, this.masterPart(), this.copy(), MessageDirection.ITERATE, getTimestamp()));
            return false;
        }

        return false;
    }

    /**
     * Deletes a replica directly from storage, if notifyMaster also removes it from the parts
     *
     * @param notifyMaster should notify master part after deletion?
     */
    @RemoteFunction
    public void deleteReplica(boolean notifyMaster) {
        if (this.state() == ReplicaState.REPLICA) {
            boolean is_deleted = deleteElement();
            if (is_deleted && notifyMaster) {
                new RemoteInvoke()
                        .toElement(decodeFeatureId("parts"), ElementType.FEATURE)
                        .hasUpdate()
                        .method("remove")
                        .withArgs(getPartId())
                        .where(MessageDirection.ITERATE)
                        .addDestination(masterPart())
                        .withTimestamp(getTimestamp())
                        .buildAndRun(storage);
            }
        }

    }

    /**
     * Sends a copy of this element as message to all parts
     *
     * @param parts where should the message be sent
     */
    public void syncReplicas(List<Short> parts) {
        if ((this.state() != ReplicaState.MASTER) || this.isHalo() || parts == null || parts.isEmpty()) return;
        cacheFeatures();
        ReplicableGraphElement cpy = this.copy();
        for (Feature<?, ?> feature : this.features) {
            if (feature.isHalo()) continue;
            Feature<?, ?> tmp = feature.copy();
            cpy.setFeature(feature.getName(), tmp);
        }
        parts.forEach(part_id -> this.storage.layerFunction.message(new GraphOp(Op.SYNC, part_id, cpy, MessageDirection.ITERATE, getTimestamp())));
    }

    @Override
    public short masterPart() {
        return this.master;
    }

    @Override
    public Boolean isHalo() {
        return this.halo;
    }

    @Override
    public List<Short> replicaParts() {
        Feature<?, ?> parts = this.getFeature("parts");
        if (Objects.isNull(parts)) return super.replicaParts();
        else {
            return (List<Short>) parts.getValue();
        }
    }

    @Override
    public Boolean isReplicable() {
        return true;
    }
}
