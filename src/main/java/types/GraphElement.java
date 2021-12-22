package types;


import annotations.AccumulatorAnnotation;
import annotations.FeatureAnnotation;
import features.Feature;
import features.ReplicableFeature;
import org.jetbrains.annotations.NotNull;
import storage.GraphStorage;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Base class for all elements is the graph (Vertex,Edge,)
 * A graph Element has an Id, a storage that it is living in, partId
 * 1 Callback -> setStorageCallback() #Initialize all
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

    @Override
    public boolean equals(Object o){
        GraphElement e =(GraphElement) o;
        return e.getClass().toString().equals(this.getClass().toString()) && this.getId().equals(e.getId());
    }

    /**
     * Send message to the next operator down the line
     * @// TODO: 03/12/2021 Can experiment with forceNextOperator to see which one yields better results 
     * @param msg
     * @param partId
     */
    public void sendMessage(GraphQuery msg, Short partId){
        this.getStorage().collect(msg.generateQueryForPart(partId),true);
    }

    /**
     * Callback when the storage actually stores this GraphELement
     * Need to update some references for the features once this step is done
     * 1. Nullify all feature/accumulators not belonging to this part
     * 2. setElement of the Features that are remaining
     * 3. Sync ReplicatedFeatures
     * @param storage GraphStorage
     */
    public void setStorageCallback(GraphStorage storage){
        this.storage = storage;
        if(storage!=null){
            this.partId = storage.getPartId();
            ArrayList<Field> featureFields = GraphElement.getFeatures(this);
            for(Field f:featureFields){
                try{
//                    if(f.isAnnotationPresent(FeatureAnnotation.class) && f.getAnnotation(FeatureAnnotation.class).level()!=-1 && f.getAnnotation(FeatureAnnotation.class).level()!=levelPart)f.set(this,null);
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

    /**
     * Callback when a new Feature update is arriving at this GraphElement
     * @param incoming
     */
    public void updateFeatureCallback(Feature.Update<?> incoming){
        try{
            Feature feature = this.getFeature(incoming.fieldName);
            feature.updateMessage(incoming);
        }
        catch(IllegalAccessException e){
            e.printStackTrace();
        }catch (NoSuchFieldException e){
            e.printStackTrace();
        }
        catch (Exception e){
            e.printStackTrace();
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


    /**
     * Get all Features up-till this class
     * @param el
     * @return
     */
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

    /**
     * Get Feature with the given filedName
     * @param fieldName name of the field of the feature
     * @return
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    public Feature getFeature(String fieldName) throws NoSuchFieldException,IllegalAccessException{
        Class <?> tmp = null;
        Field res = null;
        do{
            if(tmp==null) tmp=this.getClass();
            else tmp = tmp.getSuperclass();
            try{
                Field tmpField = tmp.getDeclaredField(fieldName);
                if(Feature.class.isAssignableFrom(tmpField.getType())){
                    res = tmpField;
                    break;
                }
            }catch (Exception e){
                // no need to do anything
            }
        }
        while(!tmp.equals(ReplicableGraphElement.class));
        if(res==null) throw new NoSuchFieldException("Field not found") ;
        return (Feature)res.get(this);
    }

    /**
     * Get the Field of a level x feature
     * @param level 0..
     * @return
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    @NotNull
    public Field getFeatureField(int level) throws NoSuchFieldException,IllegalAccessException{
        Class<?> tmp = null;
        ArrayList<Field> fields = new ArrayList<>();
        tmp = this.getClass();
        while(!tmp.equals(GraphElement.class)){
            Field[] fieldsForClass = tmp.getDeclaredFields();
            for(Field tmpField:fieldsForClass){
                if(Feature.class.isAssignableFrom(tmpField.getType()) && tmpField.isAnnotationPresent(FeatureAnnotation.class) && tmpField.getAnnotation(FeatureAnnotation.class).level()==level)return tmpField;
            }
            tmp = tmp.getSuperclass();
        }
        throw new NoSuchFieldException();
    }

    /**
     * Get Accumulator defined for the GraphElement
     * @param level 0..
     * @return
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    public Feature getAccumulator(int level) throws NoSuchFieldException,IllegalAccessException{
        Class<?> tmp = null;
        ArrayList<Field> fields = new ArrayList<>();
        tmp = this.getClass();
        while(!tmp.equals(GraphElement.class)){
            Field[] fieldsForClass = tmp.getDeclaredFields();
            for(Field tmpField:fieldsForClass){
                if(Feature.class.isAssignableFrom(tmpField.getType()) && tmpField.isAnnotationPresent(AccumulatorAnnotation.class) && tmpField.getAnnotation(AccumulatorAnnotation.class).level()==level)return (Feature) tmpField.get(this);
            }
            tmp = tmp.getSuperclass();
        }
        throw new NoSuchFieldException();
    }


}
