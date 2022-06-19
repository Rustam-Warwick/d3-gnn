package elements;

import elements.iterations.MessageDirection;
import elements.iterations.RemoteFunction;
import elements.iterations.RemoteInvoke;
import features.Set;
import org.apache.flink.api.java.tuple.Tuple2;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ReplicableGraphElement extends GraphElement {
    @Nullable
    public Short master;

    public Boolean halo = false;

    public ReplicableGraphElement() {
        super();
    }

    public ReplicableGraphElement(ReplicableGraphElement element, boolean deepCopy) {
        super(element, deepCopy);
        this.master = element.master;
        this.halo = element.halo;
    }

    public ReplicableGraphElement(String id, boolean halo, short master) {
        super(id);
        this.halo = halo;
        this.master = master;
    }

    @Override
    public ReplicableGraphElement copy() {
        return new ReplicableGraphElement(this, false);
    }

    @Override
    public ReplicableGraphElement deepCopy() {
        return new ReplicableGraphElement(this, true);
    }

    /**
     * Create the graph element. Assigns a parts feature for masters & sends sync request for replicas
     *
     * @return is_created
     */
    @Override
    public Boolean create() {
        if (state() == ReplicaState.REPLICA) clearFeatures();
        boolean is_created = createElement();
        if (is_created && !isHalo()) {
            if (state() == ReplicaState.MASTER) {
                // Add setFeature
                setFeature("parts", new Set<Short>(new ArrayList<>(), true));
            } else if (state() == ReplicaState.REPLICA) {
                // Send Query
                storage.layerFunction.message(new GraphOp(Op.SYNC, masterPart(), this), MessageDirection.ITERATE);
            }
        }
        return is_created;
    }

    /**
     * Master -> Add to part feature, send syncReplicas request
     * Replica -> Accept the sync and update the element
     *
     * @param newElement New element to sync with
     * @return (isSynced, oldElement)
     */
    @Override
    public Tuple2<Boolean, GraphElement> sync(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) {
            assert newElement.getPartId() != null;
            new RemoteInvoke()
                    .toElement(decodeFeatureId("parts"), ElementType.FEATURE)
                    .hasUpdate()
                    .method("add")
                    .addDestination(masterPart())
                    .where(MessageDirection.ITERATE)
                    .withArgs(newElement.getPartId())
                    .buildAndRun(storage);
            syncReplicas(List.of(newElement.getPartId()));
        } else if (state() == ReplicaState.REPLICA) {
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
        if (state() == ReplicaState.MASTER) {
            Tuple2<Boolean, GraphElement> tmp = updateElement(newElement);
            if (tmp.f0 && !isHalo()) {
                syncReplicas(replicaParts());
            }
            return tmp;
        } else if (state() == ReplicaState.REPLICA) {
            // Replica update, simply ignore it. SHold have been at MASTER
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
                    .buildAndRun(storage);
            return deleteElement();

        } else if (state() == ReplicaState.REPLICA) {
            assert storage != null;
            storage.layerFunction.message(new GraphOp(Op.REMOVE, masterPart(), copy()), MessageDirection.ITERATE);
            return false;
        }
        return false;
    }

    /**
     * Deletes a replica directly from storage, if notifyMaster also removes it from the parts
     *
     * @param notifyMaster should notify it master part after deletion?
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
        assert storage != null;
        if ((this.state() != ReplicaState.MASTER) || Objects.equals(isHalo(), true) || parts == null || parts.isEmpty())
            return;
        cacheFeatures();
        ReplicableGraphElement cpy = copy();
        if (features != null) {
            for (Feature<?, ?> feature : features) {
                if (feature.isHalo()) continue;
                Feature<?, ?> tmp = feature.copy();
                cpy.setFeature(feature.getName(), tmp);
            }
        }
        parts.forEach(part_id -> this.storage.layerFunction.message(new GraphOp(Op.SYNC, part_id, cpy), MessageDirection.ITERATE));
    }

    @Override
    @Nullable
    public Short masterPart() {
        return this.master;
    }

    @Override
    public Boolean isHalo() {
        return this.halo;
    }

    @Override
    public List<Short> replicaParts() {
        Feature<?, ?> parts = getFeature("parts");
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
