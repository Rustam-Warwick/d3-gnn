package plugins.debugging;

import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.enums.ElementType;
import operators.BaseWrapperOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Function for debugging the interaction of vertices in the system
 */
public class PrintHEdgePlugin extends Plugin {
    public List<String> registeredHEdges = new ArrayList<String>();

    public PrintHEdgePlugin(String... hEdgeIds) {
        Collections.addAll(registeredHEdges, hEdgeIds);
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.HYPEREDGE && registeredHEdges.contains(element.getId())) {
            BaseWrapperOperator.LOG.error(String.format("[CREATE] %s at (%s, %s) -> %s \n", element, storage.layerFunction.getCurrentPart(), storage.layerFunction.getPosition(), storage.layerFunction.currentTimestamp()));
        }
        if (element.elementType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo.f0 == ElementType.HYPEREDGE && registeredHEdges.contains(feature.attachedTo.f1)) {
                BaseWrapperOperator.LOG.error(String.format("[CREATE] Feature (%s) of Vertex (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.attachedTo.f1, getPartId(), storage.layerFunction.getPosition(), storage.layerFunction.currentTimestamp(), feature.value));
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.HYPEREDGE && registeredHEdges.contains(newElement.getId())) {
            BaseWrapperOperator.LOG.error(String.format("[UPDATE] %s at (%s, %s) -> %s \n", newElement, storage.layerFunction.getCurrentPart(), storage.layerFunction.getPosition(), storage.layerFunction.currentTimestamp()));
        }
        if (newElement.elementType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            if (feature.attachedTo != null && registeredHEdges.contains(feature.attachedTo.f1)) {
                BaseWrapperOperator.LOG.error(String.format("[UPDATE] Feature (%s) of Vertex (%s), at (%s,%s) -> %s \n Value is: %s \n\n", feature.getName(), feature.attachedTo.f1, getPartId(), storage.layerFunction.getPosition(), storage.layerFunction.currentTimestamp(), feature.value));
            }
        }
    }

}
