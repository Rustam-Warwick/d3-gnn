package plugins.debugging;

import elements.DirectedEdge;
import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.enums.ElementType;
import operators.BaseWrapperOperator;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Function for debugging the interaction of vertices in the system
 */
public class PrintVertexPlugin extends Plugin {
    public Set<String> registeredVertices = new HashSet<>();

    public PrintVertexPlugin(String... vertices) {
        super("print-vertex");
        Collections.addAll(registeredVertices, vertices);
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.getType() == ElementType.VERTEX && registeredVertices.contains(element.getId())) {
            BaseWrapperOperator.LOG.error(String.format("[CREATE] %s Vertex (%s), at (%s,%s) -> %s \n", element.state(), element.getId(), getPart(), getStorage().layerFunction.getPosition(), getStorage().layerFunction.currentTimestamp()));
        }
        if (element.getType() == ElementType.EDGE) {
            DirectedEdge e = (DirectedEdge) element;
            if (registeredVertices.contains(e.getSrc().getId()) || registeredVertices.contains(e.getDest().getId())) {
                BaseWrapperOperator.LOG.error(String.format("[CREATE] Edge (%s %s)->(%s %s), at (%s,%s) -> %s \n", e.getSrc().getId(), e.getSrc().state(), e.getDest().getId(), e.getDest().state(), getPart(), getStorage().layerFunction.getPosition(), getStorage().layerFunction.currentTimestamp()));
            }
        }
        if (element.getType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.ids.f0 == ElementType.VERTEX && registeredVertices.contains(feature.ids.f1)) {
                BaseWrapperOperator.LOG.error(String.format("[CREATE] Feature (%s) of Vertex (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.ids.f1, getPart(), getStorage().layerFunction.getPosition(), getStorage().layerFunction.currentTimestamp(), feature.value));
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.getType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            if (feature.ids.f0 == ElementType.VERTEX && registeredVertices.contains(feature.ids.f1)) {
                BaseWrapperOperator.LOG.error(String.format("[UPDATE] Feature (%s) of Vertex (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.ids.f1, getPart(), getStorage().layerFunction.getPosition(), getStorage().layerFunction.currentTimestamp(), feature.value));
            }
        }
    }

}
