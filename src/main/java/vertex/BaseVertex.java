package vertex;

import features.ReplicableArrayListFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import storage.GraphStorage;
import types.Aggregatable;
import types.GraphQuery;
import types.ReplicableGraphElement;

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
     * Initialize Feature values here
     */
    public void addVertexCallback(){
        parts = new ReplicableArrayListFeature<>("parts",this);
        parts.startTimer("3");
        parts.add(this.partId);

    }

    /**
     * Get feature of the l hop. l=0 is the initial features
     * @param l
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
