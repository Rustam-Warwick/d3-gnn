package elements;

import iterations.IterationState;
import scala.Tuple2;

import java.util.Map;

public class ReplicableGraphElement extends GraphElement {
    public short master = -1;
    public boolean halo = false;

    public ReplicableGraphElement(String id) {
        super(id);
    }

    public ReplicableGraphElement(String id, short part_id) {
        super(id, part_id);
    }

    public ReplicableGraphElement(String id, short part_id, short master){super(id, part_id); this.master = master;}

    @Override
    public Boolean createElement() {
        if (this.state() == ReplicaState.REPLICA){
            this.features.clear();
        }
        boolean is_created = super.createElement();
        if(is_created){
            if(this.state() == ReplicaState.MASTER){
                // Add setFeature
                // @todo add SetFeature here
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


        }else if(this.state() == ReplicaState.REPLICA){
            return this.updateElement(newElement);
        }

        return super.syncElement(this);

    }

    @Override
    public Tuple2<Boolean, GraphElement> externalUpdate(GraphElement newElement) {
        if(this.state() == ReplicaState.MASTER){
            Tuple2<Boolean, GraphElement> tmp = super.externalUpdate(newElement);
            return tmp;
        }
        else if(this.state() == ReplicaState.REPLICA){
            this.storage.message(new GraphOp(Op.COMMIT, this.masterPart(), newElement, IterationState.ITERATE));
            return new Tuple2<>(false, this);
        }
        else return super.externalUpdate(newElement);
    }

    public void syncReplicas(boolean skipHalo){
        if((this.state() != ReplicaState.MASTER) || (this.replicaParts().length == 0) || (this.isHalo() && skipHalo)){
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
    public short[] replicaParts() {
        return super.replicaParts();
    }

    @Override
    public Boolean isReplicable() {
        return true;
    }
}
