package plugins.debugging;

import elements.DirectedEdge;
import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.enums.ElementType;

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
        if (element.getType() == ElementType.VERTEX && (registeredVertices.contains(element.getId()) || registeredVertices.isEmpty())) {
            LOG.error(String.format("[CREATE] %s: {%s | %s}", element, getStorage().layerFunction.getPosition(), getPart()));
        }
        if (element.getType() == ElementType.EDGE) {
            DirectedEdge e = (DirectedEdge) element;
            if (registeredVertices.contains(e.getSrcId()) || (registeredVertices.contains(e.getDestId()) || registeredVertices.isEmpty())) {
                LOG.error(String.format("[CREATE] %s: {%s | %s}", element, getStorage().layerFunction.getPosition(), getPart()));
            }
        }
        if (element.getType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.ids.f0 == ElementType.VERTEX && (registeredVertices.contains(feature.ids.f1) || registeredVertices.isEmpty())) {
                LOG.error(String.format("[CREATE] %s: {%s | %s}", element, getStorage().layerFunction.getPosition(), getPart()));
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.getType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            if (feature.ids.f0 == ElementType.VERTEX && (registeredVertices.contains(feature.ids.f1) || registeredVertices.isEmpty())) {
                LOG.error(String.format("[UPDATE] %s: {%s | %s}", newElement, getStorage().layerFunction.getPosition(), getPart()));
            }
        }
    }

}
