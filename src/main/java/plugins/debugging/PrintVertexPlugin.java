package plugins.debugging;

import elements.*;
import operators.BaseWrapperOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Function for debugging the interaction of vertices in the system
 */
public class PrintVertexPlugin extends Plugin {
    public List<String> registeredVertices = new ArrayList<String>();

    public PrintVertexPlugin(String... vertices) {
        Collections.addAll(registeredVertices, vertices);
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.VERTEX && registeredVertices.contains(element.getId())) {
            BaseWrapperOperator.LOG.info(String.format("[CREATE] %s Vertex (%s), at (%s,%s) -> %s \n", element.state(), element.getId(), getPartId(), storage.layerFunction.getPosition(), element.getTimestamp()));
        }
        if (element.elementType() == ElementType.EDGE) {
            Edge e = (Edge) element;
            if (registeredVertices.contains(e.src.getId()) || registeredVertices.contains(e.dest.getId())) {
                BaseWrapperOperator.LOG.info(String.format("[CREATE] Edge (%s %s)->(%s %s), at (%s,%s) -> %s \n", e.src.getId(), e.src.state(), e.dest.getId(), e.dest.state(), getPartId(), storage.layerFunction.getPosition(), element.getTimestamp()));
            }
        }
        if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo != null && registeredVertices.contains(feature.attachedTo.f1)) {
                BaseWrapperOperator.LOG.info(String.format("[CREATE] Feature (%s) of Vertex (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.attachedTo.f1, getPartId(), storage.layerFunction.getPosition(), element.getTimestamp(), feature.value));
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            if (feature.attachedTo != null && registeredVertices.contains(feature.attachedTo.f1)) {
                BaseWrapperOperator.LOG.info(String.format("[UPDATE] Feature (%s) of Vertex (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.attachedTo.f1, getPartId(), storage.layerFunction.getPosition(), newElement.getTimestamp(), feature.value));
            }
        }
    }

}
