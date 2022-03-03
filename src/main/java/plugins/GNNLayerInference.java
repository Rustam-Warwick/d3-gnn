package plugins;

import elements.Plugin;
import elements.GraphElement;

public class GNNLayerInference  extends Plugin {
    public GNNLayerInference(String id) {
        super(id);
    }
    public GNNLayerInference(){
        super();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        System.out.println(element);
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {

    }
}
