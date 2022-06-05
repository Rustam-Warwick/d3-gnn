package plugins.metrics;

import elements.ElementType;
import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import org.apache.flink.metrics.Meter;

public class ThroughputMetric extends Plugin {
    private transient Meter myMeter;

    public ThroughputMetric() {
        super("throughput-metric");
    }

    @Override
    public void open() {
        super.open();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if ("feature".equals(feature.getName()) && feature.attachedTo.f0 == ElementType.VERTEX) {
                // Embedding detected
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.addElementCallback(newElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            if ("feature".equals(feature.getName()) && feature.attachedTo.f0 == ElementType.VERTEX) {
                // Embedding detected
            }
        }
    }
}
