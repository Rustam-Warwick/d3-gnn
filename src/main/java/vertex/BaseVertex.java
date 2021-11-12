package vertex;

import features.ReplicableArrayListFeature;
import part.BasePart;
import storage.GraphStorage;
import types.GraphElement;
import types.GraphQuery;
import types.ReplicableGraphElement;

abstract public class BaseVertex extends ReplicableGraphElement {

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
        parts.add(this.partId);
//        parts.startTimer("3");
    }

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
