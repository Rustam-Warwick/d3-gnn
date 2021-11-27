package types;


import features.Feature;
import features.ReplicableArrayListFeature;
import features.ReplicableFeature;
import features.ReplicableTensorFeature;
import org.jetbrains.annotations.NotNull;
import storage.GraphStorage;

import java.lang.reflect.Field;
import java.util.ArrayList;

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
        super.setStorageCallback(storage);
        parts.add(this.partId);
    }

    /**
     * Send message to specific part
     * @param msg
     * @param partId
     */
    public void sendMessage(GraphQuery msg, Short partId){
        this.getStorage().getPart().collect(msg.generateQueryForPart(partId),false);
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

    public void updateFeature(Feature.Update<?> incoming){
        try{
            Field myFeature = ReplicableGraphElement.getFeature(this,incoming.fieldName);
            Feature feature = (Feature) myFeature.get(this);
            feature.updateMessage(incoming);
        }
        catch(IllegalAccessException e){
            System.out.println(e.getMessage());
            System.out.println("Error");
        }catch (NoSuchFieldException e){
            System.out.println(e.getMessage());
            System.out.println("Error");
        }
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



    @NotNull
    public static Field getFeature(ReplicableGraphElement el,String fieldName) throws NoSuchFieldException{
        Class <?> tmp = null;
        Field res = null;
        do{
            if(tmp==null) tmp=el.getClass();
            else tmp = tmp.getSuperclass();
            try{
                Field tmpField = tmp.getDeclaredField(fieldName);
                res = tmpField;
                break;
            }catch (Exception e){
                // no need to do anything
            }
        }
        while(!tmp.equals(ReplicableGraphElement.class));
        if(res==null) throw new NoSuchFieldException("Field not found") ;
        return res;
    }
}
