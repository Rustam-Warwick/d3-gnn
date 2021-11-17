package vertex;

import features.ReplicableArrayListFeature;
import features.ReplicableFeature;
import features.ReplicableTensorFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.shade.jackson.databind.ser.Serializers;
import storage.GraphStorage;
import types.Aggregatable;
import types.GraphElement;
import types.GraphQuery;
import types.ReplicableGraphElement;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

abstract public class BaseVertex extends ReplicableGraphElement implements Aggregatable {

    public ReplicableArrayListFeature<Short> parts = null;

    public BaseVertex(String id, GraphStorage storage) {
        super(id, storage);
    }

    public BaseVertex(String id) {
        super(id);
    }

    public BaseVertex() {
        super();
    }

    /**
     * Initialize Feature values here need to recreate so that data is fetched from this vertex
     */
    public void addVertexCallback(){
        ArrayList<Field> remoteFeatures = ReplicableGraphElement.getReplicableFeatures(this);
        for(Field f:remoteFeatures){
            try{
                ReplicableFeature a = (ReplicableFeature) f.get(this);
                if(a!=null){
                    // There is some data that was sent
                    f.set(this,f.getType().getConstructors()[0].newInstance(f.getName(),this,a.value));
                }else{
                    // Initialize the default constructor
                    f.set(this,f.getType().getConstructor(String.class, GraphElement.class).newInstance(f.getName(),this));
                }
            }catch (Exception e){
                System.out.println("Exception haha");

            }
        }

        parts.add(this.partId);
//        parts.startTimer("3");

    }

    /**
     * Get feature of the l hop. l=0 is the initial features
     * @return
     */
    @Override
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

    abstract public BaseVertex copy();
}
