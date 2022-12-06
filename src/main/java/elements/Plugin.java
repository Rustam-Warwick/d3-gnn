package elements;

import elements.enums.CopyContext;
import operators.interfaces.RichGraphElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * It always lives in the <strong>Operator State</strong> so it is not in the life-cycle of the logical keys
 * Furthermore, Plugins cam be thought of as extension of Storage, as they are allowed to created their own <strong>KeyedStates</strong>
 * </p>
 */
@SuppressWarnings("unused")
public class Plugin extends GraphElement implements RichGraphElement {

    /**
     * Plugin Logger
     */
    protected final static Logger LOG = LoggerFactory.getLogger(Plugin.class);

    /**
     * ID of this plugin, should be unique per storage
     */
    final public String id;

    /**
     * Is this Plugin Active
     */
    public boolean IS_ACTIVE = true;

    public Plugin() { id= null;}

    public Plugin(String id) {
        this.id = id;
    }

    public Plugin(String id, boolean IS_ACTIVE) {
        this.id = id;
        this.IS_ACTIVE = IS_ACTIVE;
    }


    public String getId() {
        return id;
    }

    /**
     * Stop this plugin
     */
    public void stop() {
        IS_ACTIVE = false;
    }

    /**
     * Start this plugin
     */
    public void start() {
        IS_ACTIVE = true;
    }

    @Override
    public GraphElement copy(CopyContext context) {
        throw new IllegalStateException("Plugins cannot be copied");
    }
    // ----------------------- CALLBACKS --------------------

    /**
     * Callback when a graph element is created
     */
    public void addElementCallback(GraphElement element) {
        // pass
    }

    /**
     * Callback when a graph element is updated
     */
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        // pass
    }

    /**
     * Callback when a graph element is removed
     */
    public void deleteElementCallback(GraphElement deletedElement) {
        // pass
    }

}
