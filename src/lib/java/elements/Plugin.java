package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphListener;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.apache.flink.streaming.api.operators.graph.interfaces.RichGraphProcess;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * It always lives in the <strong>Operator State</strong> so it is not in the life-cycle of the logical keys
 * Furthermore, Plugins cam be thought of as extension of Storage, as they are allowed to created their own <strong>KeyedStates</strong>
 * </p>
 */
@SuppressWarnings("unused")
public class Plugin extends GraphElement implements RichGraphProcess, GraphListener {

    /**
     * Plugin Logger
     */
    protected final static Logger LOG = LoggerFactory.getLogger(Plugin.class);

    /**
     * ID of this plugin, should be unique per storage
     */
    final public String id;

    /**
     * Is this Plugin currently running
     */
    public boolean running;

    /**
     * Reference to the {@link GraphRuntimeContext}
     */
    public transient GraphRuntimeContext graphRuntimeContext;

    public Plugin() {
        this(null, true);
    }

    public Plugin(String id) {
        this(id, true);
    }

    public Plugin(String id, boolean running) {
        this.id = id;
        this.running = running;
    }

    /**
     * {@inheritDoc}
     * Throws {@link  IllegalStateException}
     */
    @Override
    public GraphElement copy(CopyContext context) {
        throw new IllegalStateException("Plugins should not be copied");
    }

    /**
     * {@inheritDoc}
     * Throws {@link IllegalStateException}
     */
    @Override
    public Feature<?, ?> getFeature(String name) {
        throw new IllegalStateException("Plugins do not have Features instead save state in the Plugin intself");
    }

    /**
     * {@inheritDoc}
     * Throws {@link IllegalStateException}
     */
    @Override
    public Boolean containsFeature(String name) {
        throw new IllegalStateException("Plugins do not have Features instead save state in the Plugin intself");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphRuntimeContext getRuntimeContext() {
        return graphRuntimeContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setRuntimeContext(RuntimeContext t) {
        graphRuntimeContext = (GraphRuntimeContext) t;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return ElementType.PLUGIN;
    }
}
