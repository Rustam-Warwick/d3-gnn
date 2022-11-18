package plugins.debugging;

import elements.DEdge;
import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.enums.ElementType;
import operators.BaseWrapperOperator;

public class GraphGNNPlugin extends Plugin {
    public GraphGNNPlugin() {
        super("print_graph_gnn");
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.VERTEX) {
            BaseWrapperOperator.LOG.info(String.format("[CREATE] %s Vertex (%s), at (%s,%s) -> %s \n", element.state(), element.getId(), getPartId(), storage.layerFunction.getPosition(), storage.layerFunction.currentTimestamp()));
        }
        if (element.elementType() == ElementType.EDGE) {
            DEdge e = (DEdge) element;
            BaseWrapperOperator.LOG.info(String.format("[CREATE] Edge (%s %s)->(%s %s), at (%s,%s) -> %s \n", e.getSrc().getId(), e.getSrc().state(), e.getDest().getId(), e.getDest().state(), getPartId(), storage.layerFunction.getPosition(), storage.layerFunction.currentTimestamp()));
        }
        if (element.elementType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo.f0 == ElementType.VERTEX) {
                BaseWrapperOperator.LOG.info(String.format("[CREATE] Feature (%s) of Vertex (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.attachedTo.f1, getPartId(), storage.layerFunction.getPosition(), storage.layerFunction.currentTimestamp(), feature.value));
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            if (feature.attachedTo.f0 == ElementType.VERTEX) {
                BaseWrapperOperator.LOG.error(String.format("[UPDATE] Feature (%s) of Vertex (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.attachedTo.f1, getPartId(), storage.layerFunction.getPosition(), storage.layerFunction.currentTimestamp(), feature.value));
            }
        }
    }
}
