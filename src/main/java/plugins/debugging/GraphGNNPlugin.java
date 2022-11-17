package plugins.debugging;

import elements.GraphElement;
import elements.Plugin;

public class GraphGNNPlugin extends Plugin {
    public GraphGNNPlugin() {
        super("print_graph_gnn");
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
