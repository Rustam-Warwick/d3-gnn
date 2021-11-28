package features;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.indexing.conditions.GreaterThan;
import types.GraphElement;

public class ReplicableMinAggregator extends ReplicableAggregator<INDArray>  {


    public ReplicableMinAggregator() {
        super();
    }

    public ReplicableMinAggregator(String fieldName) {
        super(fieldName);
    }

    public ReplicableMinAggregator(String fieldName, GraphElement element) {
        super(fieldName, element,null);
    }

    public ReplicableMinAggregator(String fieldName, GraphElement element, INDArray value) {
        super(fieldName, element, value);
    }

    @Override
    public void addNewElement(INDArray e){
        this.editHandler(item->{
            item.value.assignIf(e, new GreaterThan());
            return true;
        });
    }
    @Override
    public void updateElements(INDArray ...e){
        if(e.length==1){
            this.addNewElement(e[0]);
            return;
        }
        int i = e.length -2;
        while(i>0){
            e[i].assignIf(e[i+1],new GreaterThan());
            i--;
        }
        this.addNewElement(e[0]);
    }

    @Override
    public void setValue(INDArray value) {
        this.editHandler(item->{
            item.value = value;
            return true;
        });
    }
}
