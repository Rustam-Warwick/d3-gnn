package elements.features;

import elements.ElementType;
import elements.GraphElement;
import elements.ReplicableGraphElement;

public class Feature<T> extends ReplicableGraphElement {
    public Object value = null;
    public GraphElement element = null;

    public Feature(String id, Object value){
        super(id);
        this.value = value;
    }

    public Feature(String id) {
        super(id);
    }

    public Feature(String id, short part_id) {
        super(id, part_id);
    }

    @Override
    public Boolean createElement() {
        return super.createElement();
    }

    @Override
    public ElementType elementType() {
       return ElementType.FEATURE;
    }
}
