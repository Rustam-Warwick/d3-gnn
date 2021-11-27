package vertex;

import features.ReplicableArrayListFeature;
import features.ReplicableFeature;
import storage.GraphStorage;
import types.IncrementalAggregatable;
import types.GraphElement;
import types.GraphQuery;
import types.ReplicableGraphElement;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;

abstract public class BaseVertex extends ReplicableGraphElement implements IncrementalAggregatable {


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
