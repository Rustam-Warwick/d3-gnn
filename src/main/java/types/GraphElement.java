package types;


import features.Feature;
import features.ReplicableFeature;
import org.jetbrains.annotations.NotNull;
import org.nd4j.shade.guava.graph.Graph;
import storage.GraphStorage;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Base class for all elements is the graph (Vertex,Edge,)
 * A graph Element has an Id, a storage that it is living in, partId
 */
abstract public class GraphElement{
    public String id = null;
    public transient GraphStorage storage = null;
    public Short partId = null;

    public GraphElement(String id){
        this.setId(id);
        this.storage = null;
    }
    public GraphElement(){
        this.id = null;
        this.storage = null;
    }
    public GraphElement(GraphElement e){
        this.id = e.id;
        this.partId = e.partId;
        this.storage = e.storage;
    }

    public String getId(){
        return this.id;
    }
    public void setId(String id){
        this.id = id;
    }

    /**
     * Callback when the storage actually stores this GraphELement
     * Need to update some references for the features once this step is done
     * @param storage
     */
    public void setStorageCallback(GraphStorage storage){
        this.storage = storage;
        if(storage!=null){
            this.partId = storage.getPart().getPartId();
            ArrayList<Field> featureFields = GraphElement.getFeatures(this);
            for(Field f:featureFields){
                try{
                     Feature feature = (Feature)f.get(this);
                     if(!Objects.isNull(feature))feature.setElement(this);
                     if(feature instanceof ReplicableFeature){
                         ((ReplicableFeature)feature).sync();
                     }
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }

            }

        }
    }

    public GraphStorage getStorage() {
        return storage;
    }

    public void setPartId(Short partId) {
        this.partId = partId;
    }

    public Short getPartId() {
        return partId;
    }

    @NotNull
    public static ArrayList<Field> getFeatures(GraphElement el){
        Class<?> tmp = null;
        ArrayList<Field> fields = new ArrayList<>();
        tmp = el.getClass();
        while(!tmp.equals(GraphElement.class)){
            Field[] fieldsForClass = tmp.getDeclaredFields();
            for(Field tmpField:fieldsForClass){
                if(Feature.class.isAssignableFrom(tmpField.getType()))fields.add(tmpField);
            }
            tmp = tmp.getSuperclass();
        }
        return fields;
    }

    @Override
    public boolean equals(Object o){
        GraphElement e =(GraphElement) o;
        return e.getClass().toString().equals(this.getClass().toString()) && this.getId().equals(e.getId());
    }

}
