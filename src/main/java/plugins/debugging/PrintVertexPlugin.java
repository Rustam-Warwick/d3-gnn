package plugins.debugging;

import elements.*;

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
            System.out.format("[CREATE] %s Vertex (%s), at (%s,%s) -> %s \n", element.state(), element.getId(), getPartId(), storage.layerFunction.getPosition(), element.getTimestamp());
        }
        if (element.elementType() == ElementType.EDGE) {
            Edge e = (Edge) element;
            if (registeredVertices.contains(e.src.getId()) || registeredVertices.contains(e.dest.getId())) {
                System.out.format("[CREATE] Edge (%s), at (%s,%s) -> %s \n", element.getId(), getPartId(), storage.layerFunction.getPosition(), element.getTimestamp());
            }
        }
        if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (registeredVertices.contains(feature.attachedTo.f1)) {
                System.out.format("[CREATE] Feature (%s) of Vertex (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.attachedTo.f1, getPartId(), storage.layerFunction.getPosition(), element.getTimestamp(), feature.getValue());
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            if (registeredVertices.contains(feature.attachedTo.f1)) {
                System.out.format("[UPDATE] Feature (%s) of Vertex (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.attachedTo.f1, getPartId(), storage.layerFunction.getPosition(), newElement.getTimestamp(), feature.getValue());
            }
        }
    }

}
