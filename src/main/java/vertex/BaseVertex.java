package vertex;
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



    abstract public BaseVertex copy();
}
