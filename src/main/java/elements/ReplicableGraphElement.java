package elements;

import features.Set;
import iterations.IterationType;
import iterations.RemoteInvoke;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ReplicableGraphElement extends GraphElement {
    public short master;
    public boolean halo;

    public ReplicableGraphElement() {
        super();
        this.master = -1;
        this.halo = false;
    }

    public ReplicableGraphElement(String id) {
        super(id);
        this.master = -1;
        this.halo = false;
    }

    public ReplicableGraphElement(String id, boolean halo) {
        super(id);
        this.halo = halo;
        this.master = -1;
    }

    public ReplicableGraphElement(String id, boolean halo, short master) {
        super(id);
        this.halo = halo;
        this.master = master;
    }

    @Override
    public GraphElement copy() {
        ReplicableGraphElement tmp = new ReplicableGraphElement(this.id, this.halo, this.master);
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        ReplicableGraphElement tmp = new ReplicableGraphElement(this.id, this.halo, this.master);
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    // Main Logical Stuff
    @Override
    public Boolean createElement() {
        if (this.state() == ReplicaState.REPLICA) {
            this.features.clear();
        }
        boolean is_created = super.createElement();
        if (is_created) {
            if (this.state() == ReplicaState.MASTER) {
                // Add setFeature
                this.setFeature("parts", new Set<Short>(new ArrayList<>(), true));
            } else {
                // Send Query
                this.storage.layerFunction.message(new GraphOp(Op.SYNC, this.masterPart(), this, IterationType.ITERATE));
            }
        }
        return is_created;
    }

    @Override
    public Tuple2<Boolean, GraphElement> syncElement(GraphElement newElement) {
        if (this.state() == ReplicaState.MASTER) {
            new RemoteInvoke()
                    .toElement(decodeFeatureId("parts"), ElementType.FEATURE)
                    .hasUpdate()
                    .method("add")
                    .toDestination(masterPart())
                    .where(IterationType.ITERATE)
                    .withArgs(newElement.getPartId())
                    .buildAndRun(storage);
            syncReplicas(List.of(newElement.getPartId()));
        } else if (this.state() == ReplicaState.REPLICA) {
            return this.updateElement(newElement);
        }

        return super.syncElement(this); // Do nothing
    }

    @Override
    public Tuple2<Boolean, GraphElement> externalUpdate(GraphElement newElement) {
        if (this.state() == ReplicaState.MASTER) {
            Tuple2<Boolean, GraphElement> tmp = super.externalUpdate(newElement);
            if (tmp._1) this.syncReplicas(replicaParts());
            return tmp;
        } else if (this.state() == ReplicaState.REPLICA) {
            this.storage.layerFunction.message(new GraphOp(Op.COMMIT, this.masterPart(), newElement, IterationType.ITERATE));
            return new Tuple2<>(false, this);
        } else return super.externalUpdate(newElement);
    }

    public void syncReplicas(List<Short> parts) {
        if ((this.state() != ReplicaState.MASTER) || this.isHalo() || parts == null || parts.isEmpty()) return;
        cacheFeatures();
        ReplicableGraphElement cpy = (ReplicableGraphElement) this.copy();
        for (Feature feature : this.features) {
            if (feature.isHalo()) continue;
            Feature tmp = (Feature) feature.copy();
            cpy.setFeature(feature.getFieldName(), tmp);
        }

        parts.forEach(part_id -> this.storage.layerFunction.message(new GraphOp(Op.SYNC, part_id, cpy, IterationType.ITERATE)));

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
        Set<Short> parts = (Set<Short>) this.getFeature("parts");
        if (Objects.isNull(parts)) return super.replicaParts();
        else {
            return parts.getValue();
        }
    }

    @Override
    public Boolean isReplicable() {
        return true;
    }
}
