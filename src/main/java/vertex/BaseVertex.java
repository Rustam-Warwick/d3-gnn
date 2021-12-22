package vertex;
import features.Feature;
import features.ReplicableFeature;
import storage.GraphStorage;
import types.ReplicableGraphElement;

/**
 * Dummy BaseVertex Class. Nothing Special except for a copy function
 */
abstract public class BaseVertex extends ReplicableGraphElement{


    public BaseVertex(String id) {
        super(id);
    }

    public BaseVertex() {
        super();
    }

    public BaseVertex(BaseVertex e){
        super(e);
    }

    /**
     * If the Vertex is added to the storage engine
     * call the callback of aggregators waiting for this
     * @param storage
     */
    @Override
    public void setStorageCallback(GraphStorage storage) {
        super.setStorageCallback(storage);
    }

    /**
     * If a feature of this vertex is updated call the aggregator functions
     * @param incoming
     */
    @Override
    public void updateFeatureCallback(Feature.Update<?> incoming) {
        Integer tmp = incoming.lastModified;
        super.updateFeatureCallback(incoming);
        try{
            Feature feature = this.getFeature(incoming.fieldName);
            if(feature instanceof ReplicableFeature && ((ReplicableFeature) feature).lastModified.get()==tmp)return; // No update happened since last time
        }catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    abstract public BaseVertex copy();
}
