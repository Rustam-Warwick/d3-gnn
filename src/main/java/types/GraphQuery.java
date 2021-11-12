package types;


import edge.BaseEdge;
import org.apache.flink.api.java.typeutils.PojoField;
import vertex.BaseVertex;

import java.util.Arrays;
import java.util.List;

public class GraphQuery {
    public enum OPERATORS {NONE, ADD, REMOVE, UPDATE, SYNC}

    public Object element = null; // Element over which we are querying
    public OPERATORS op = OPERATORS.NONE;
    public Short part = null; // Which part it should be directed to
    public GraphQuery() {
        this.element = null;
        this.part = null;
    }

    public GraphQuery(Object element) {
        this.element = element;
    }

    public GraphQuery changeOperation(OPERATORS op) {
        this.op = op;
        return this;
    }
    public GraphQuery toPart(Short part){
        this.part = part;
        return this;
    }

    public static boolean isVertex(GraphQuery el) {
        return el.element instanceof BaseVertex;
    }

    public static boolean isEdge(GraphQuery el) {
        return el.element instanceof BaseEdge;
    }
    public GraphQuery generateQueryForPart(Short part){
        return new GraphQuery(this.element).changeOperation(this.op).toPart(part);
    }
}
