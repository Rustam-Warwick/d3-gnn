package elements;

import features.Set;
import iterations.IterationState;
import iterations.Rpc;
import scala.Tuple2;

import java.util.*;

public class ReplicableGraphElement extends GraphElement {
    public short master = -1;
    public boolean halo = false;

    public ReplicableGraphElement(){
        super();
    }
    public ReplicableGraphElement(String id) {
        super(id);
    }

    public ReplicableGraphElement(String id, boolean halo){
        super(id);
        this.halo = halo;
    }

    @Override
    public GraphElement copy() {
        ReplicableGraphElement tmp = new ReplicableGraphElement(this.id, this.halo);
        tmp.setPartId(this.getPartId());
        tmp.setStorage(this.storage);
        tmp.features.putAll(this.features);
        return tmp;
    }

    // Main Logical Stuff
    @Override
    public Boolean createElement() {
        if (this.state() == ReplicaState.REPLICA){
            this.features.clear();
        }
        boolean is_created = super.createElement();
        if(is_created){
            if(this.state() == ReplicaState.MASTER){
                // Add setFeature
                this.setFeature("parts", new Set<Short>(new HashSet(Arrays.asList(this.partId))));
            }
            else{
                // Send Query
                this.storage.message(new GraphOp(Op.SYNC, this.masterPart(), this, IterationState.ITERATE));
            }
        }
        return is_created;
    }

    @Override
    public Tuple2<Boolean, GraphElement> syncElement(GraphElement newElement) {
        if(this.state() == ReplicaState.MASTER){
            Set<Integer> tmp = (Set<Integer>) this.getFeature("parts");
            Rpc.call(tmp,"add",newElement.getPartId());
            this.syncReplica(newElement.getPartId());

        }else if(this.state() == ReplicaState.REPLICA){
            return this.updateElement(newElement);
        }

        return super.syncElement(this);

    }

    @Override
    public Tuple2<Boolean, GraphElement> externalUpdate(GraphElement newElement) {
        if(this.state() == ReplicaState.MASTER){
            Tuple2<Boolean, GraphElement> tmp = super.externalUpdate(newElement);
            if(tmp._1)this.syncReplicas(true);
            return tmp;
        }
        else if(this.state() == ReplicaState.REPLICA){
            this.storage.message(new GraphOp(Op.COMMIT, this.masterPart(), newElement, IterationState.ITERATE));
            return new Tuple2<>(false, this);
        }
        else return super.externalUpdate(newElement);
    }

    public void syncReplicas(boolean skipHalo){
        if((this.state() != ReplicaState.MASTER) ||  (this.isHalo() && skipHalo)){
            return;
        }
        this.cacheFeatures();
        ReplicableGraphElement cpy = (ReplicableGraphElement) this.copy();
        cpy.features.clear();
        for(Map.Entry<String, Feature> feature: this.features.entrySet()){
            if(skipHalo && feature.getValue().isHalo())continue;

        }
    }

    public void syncReplica(short part_id){

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
    public Iterator<Short> replicaParts() {
        Set<Short> parts = (Set<Short>) this.getFeature("parts");
        if(Objects.isNull(parts)) return super.replicaParts();
        else return parts.getValue().iterator();
    }

    @Override
    public Boolean isReplicable() {
        return true;
    }
}
