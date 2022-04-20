package plugins;

import elements.ElementType;
import elements.GraphElement;
import elements.Plugin;

public class NegativeSampler extends Plugin {
    double p = 0.02;

    public NegativeSampler() {
        super("negative-sampler");
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.EDGE) {

        }
    }
}
