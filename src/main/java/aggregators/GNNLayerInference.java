package aggregators;

import elements.Aggregator;
import elements.GraphElement;

public class GNNLayerInference  extends Aggregator {
    public GNNLayerInference(String id) {
        super(id);
    }
    public GNNLayerInference(){
        super();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
    }
}
