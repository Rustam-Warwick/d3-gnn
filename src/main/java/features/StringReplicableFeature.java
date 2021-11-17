package features;
import types.GraphElement;
import types.SerialFunction;



public class StringReplicableFeature extends ReplicableFeature<String>{
    public StringReplicableFeature() {
        super();
    }

    public StringReplicableFeature(String fieldName) {
        super(fieldName);
    }

    public StringReplicableFeature(String fieldName, GraphElement element) {
        super(fieldName, element,"");
    }

    public StringReplicableFeature(String fieldName, GraphElement element, String value) {
        super(fieldName, element, value);
    }

    public void add(String s){
        SerialFunction<ReplicableFeature<String>,Boolean> fn = item->{
            item.value = item.value + s;
            return true;
        };
        this.editHandler(fn);
    }
}
