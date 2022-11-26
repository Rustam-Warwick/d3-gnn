package plugins.debugging;

import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.enums.ElementType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Function for debugging the interaction of vertices in the system
 */
public class PrintHEdgePlugin extends Plugin {
    public List<String> registeredHEdges = new ArrayList<String>();

    public PrintHEdgePlugin(String... hEdgeIds) {
        super("print-hyperedge");
        Collections.addAll(registeredHEdges, hEdgeIds);
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.getType() == ElementType.HYPEREDGE && registeredHEdges.contains(element.getId())) {
            LOG.error(String.format("[CREATE] %s at (%s, %s) -> %s \n", element, getStorage().layerFunction.getCurrentPart(), getStorage().layerFunction.getPosition(), getStorage().layerFunction.currentTimestamp()));
        }
        if (element.getType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.ids.f0 == ElementType.HYPEREDGE && registeredHEdges.contains(feature.ids.f1)) {
                LOG.error(String.format("[CREATE] Feature (%s) of HyperEdge (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.ids.f1, getPart(), getStorage().layerFunction.getPosition(), getStorage().layerFunction.currentTimestamp(), feature.value));
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.getType() == ElementType.HYPEREDGE && registeredHEdges.contains(newElement.getId())) {
            LOG.error(String.format("[UPDATE] %s at (%s, %s) -> %s \n", newElement, getStorage().layerFunction.getCurrentPart(), getStorage().layerFunction.getPosition(), getStorage().layerFunction.currentTimestamp()));
        }
        if (newElement.getType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            if (feature.ids != null && registeredHEdges.contains(feature.ids.f1)) {
                LOG.error(String.format("[UPDATE] Feature (%s) of HyperEdge (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.ids.f1, getPart(), getStorage().layerFunction.getPosition(), getStorage().layerFunction.currentTimestamp(), feature.value));
            }
        }
    }

}
