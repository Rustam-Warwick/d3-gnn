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

    public ReplicableGraphElement(String id, boolean halo, Short master) {
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
     * Create the graph element. Assigns a parts feature for masters & sends sync request for replicas.
     * First send the sync messages then do the synchronization logic
     */
    @Override
    public void create() {
        if (state() == ReplicaState.REPLICA) clearFeatures();
        if(!isHalo() && state() == ReplicaState.REPLICA) storage.layerFunction.message(new GraphOp(Op.SYNC, masterPart(), this), MessageDirection.ITERATE);
        createElement(); // Delaying the plugin callback for later time
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
            assert newElement.getPartId() != null;
            if(!containsFeature("parts")) setFeature("parts", new Set<Short>(new ArrayList<>(), true)); // Create parts only there clearly is a replica
            new RemoteInvoke()
                    .toElement(Feature.encodeAttachedFeatureId("parts", getId()), ElementType.FEATURE)
                    .hasUpdate()
                    .method("add")
                    .addDestination(masterPart())
                    .where(MessageDirection.ITERATE)
                    .withArgs(newElement.getPartId())
                    .buildAndRun(storage);
            syncReplicas(List.of(newElement.getPartId()));
        } else if (state() == ReplicaState.REPLICA) {
            updateElement(newElement, null);
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
            Tuple2<Boolean, GraphElement> tmp = updateElement(newElement, null);
            if (tmp.f0 && !isHalo()) syncReplicas(replicaParts());
        } else throw new IllegalStateException("No one should receive updates other than MASTER");
    }

    /**
     * master -> Send delete message to replica, actually delete the element from master immediately
     * replica -> Redirect this message to master, replica deletions are happening through RMI deleteReplica
     *
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
            deleteElement();
        } else if (state() == ReplicaState.REPLICA) {
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
            boolean is_deleted = deleteElement();
            if (is_deleted) {
                if(notifyMaster) new RemoteInvoke()
                        .toElement(Feature.encodeAttachedFeatureId("parts", getId()), ElementType.FEATURE)
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
        if ((state() != ReplicaState.MASTER) || !isReplicable()|| Objects.equals(isHalo(), true) || parts == null || parts.isEmpty())
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
        if(!containsFeature("parts")) return super.replicaParts();
        return (List<Short>) getFeature("parts").getValue(); // @implNote Never create other Feature with the name parts
    }

    @Override
    public Boolean isReplicable() {
        return true;
    }
}
