package plugins.debugging;

import elements.GraphElement;
import elements.Plugin;

/**
 * Function for debugging all the callbacks in the system
 */
public class LogCallbacks extends Plugin {

    public LogCallbacks() {
        super("print-callbacks");
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        LOG.error(String.format("[CREATE] %s: {%s | %s}", element, getRuntimeContext().getPosition(), getPart()));
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        LOG.error(String.format("[UPDATE] %s: {%s | %s}", newElement, getRuntimeContext().getPosition(), getPart()));
    }

}
