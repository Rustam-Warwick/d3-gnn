package types;


import features.Feature;
import features.ReplicableArrayListFeature;
import features.ReplicableFeature;
import features.ReplicableTensorFeature;
import org.jetbrains.annotations.NotNull;
import storage.GraphStorage;

import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 * Simple SubClass of GraphElement that defines a parts ReplicableArray and stores masterPart information
 * of the GraphElement
 */
abstract public class ReplicableGraphElement extends GraphElement {
    /**
     * -1 -> UNDEFINED
     * null -> MASTER
     * otherwise -> Replica storing ID of MASTER
     */
    public Short masterPart;
    public ReplicableArrayListFeature<Short> parts;

    public enum STATE {NONE,REPLICA,MASTER}


    public ReplicableGraphElement(){
        super();
        this.masterPart=-1;
        this.parts = new ReplicableArrayListFeature<>("parts",this);
    }
    public ReplicableGraphElement(String id){
        super(id);
        this.masterPart=-1;
        this.parts = new ReplicableArrayListFeature<>("parts",this);
    }

    public ReplicableGraphElement(ReplicableGraphElement e){
        super(e);
        this.masterPart = e.masterPart;
        this.parts = e.parts;
    }

    @Override
    public void setStorageCallback(GraphStorage storage) {
        parts.add(this.partId);
        super.setStorageCallback(storage); // Sync is happening there anyway
    }






    /**
     * Send message to replicas as well as some optional other parts
     * @param msg
     * @param alsoSendHere
     */
    public void sendMessageToReplicas(GraphQuery msg, Short... alsoSendHere) {
        this.parts.getValue().whenComplete((item,err)->{
            for(Short i:item){
                if(i.equals(this.partId))continue;
                this.sendMessage(msg,i);
            }
        });
        for(Short i: alsoSendHere){
            this.sendMessage(msg,i);
        }

    }

    /**
     * Send message to master node IF this guy is a REPLICA
     * @param msg
     * @param alsoSendHere
     */
    public void sendMessageToMaster(GraphQuery msg, Short ...alsoSendHere){
        if(this.masterPart!=null && this.masterPart >=0)this.sendMessage(msg,this.masterPart);
        for(Short tmp:alsoSendHere)this.sendMessage(msg,tmp);
    }



    public STATE getState(){
        if(this.masterPart==null)return STATE.MASTER;
        else if(this.masterPart==-1)return STATE.NONE;
        return STATE.REPLICA;
    }
    public Short getMasterPart() {
        return masterPart;
    }

    public void setMasterPart(Short masterPart) {
        this.masterPart = masterPart;
    }




}
