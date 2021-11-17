package types;


import features.Feature;
import features.ReplicableFeature;
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
    public Short masterPart = -1;

    public enum STATE {NONE,REPLICA,MASTER}

    public ReplicableGraphElement(){
        super();
        this.masterPart=-1;
    }
    public ReplicableGraphElement(String id){
        super(id);
        this.masterPart=-1;
    }
    public ReplicableGraphElement(String id, GraphStorage storage){
        super(id,storage);
        this.masterPart=-1;
    }
    public ReplicableGraphElement(String id, GraphStorage storage, Short masterPart){
        super(id,storage);
        this.masterPart = masterPart;
    }
    public void sendMessage(GraphQuery msg, Short partId){
        this.getStorage().getPart().collect(msg.generateQueryForPart(partId));
    }
    abstract public void sendMessageToReplicas(GraphQuery msg, Short ...alsoSendHere);


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
        }catch (NoSuchFieldException e){
            System.out.println(e.getMessage());
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
    public static ArrayList<Field> getReplicableFeatures(ReplicableGraphElement el){
        Class<?> tmp = null;
        ArrayList<Field> fields = new ArrayList<>();
        do{
            if(tmp==null) tmp=el.getClass();
            else tmp = tmp.getSuperclass();
            Field[] fieldsForClass = tmp.getDeclaredFields();
            for(Field tmpField:fieldsForClass){
                if(ReplicableFeature.class.isAssignableFrom(tmpField.getType()))fields.add(tmpField);
            }
        }
        while(!tmp.equals(ReplicableGraphElement.class));
        return fields;
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
