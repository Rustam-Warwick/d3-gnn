package helpers;

import elements.GraphOp;
import org.apache.flink.api.java.functions.KeySelector;

public class ElementIdSelector implements KeySelector<GraphOp, String> {
    @Override
    public String getKey(GraphOp value) throws Exception {
//        if(value.element.elementType() == ElementType.FEATURE){
//            Feature<?,?> f = (Feature) value.element;
//            if(f.attachedTo._1 != ElementType.NONE) return f.attachedTo._2;
//        }
        return value.element.getId();
    }
}
