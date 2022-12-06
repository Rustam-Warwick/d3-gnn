package plugins.debugging;

import elements.GraphElement;
import elements.Plugin;

/**
 * Function for debugging the interaction of vertices in the system
 */
public class LogCallbacksPlugin extends Plugin {

    public LogCallbacksPlugin(String... vertices) {
        super("print-vertex");
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
