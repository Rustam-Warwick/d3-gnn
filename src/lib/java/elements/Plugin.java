package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphListener;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.apache.flink.streaming.api.operators.graph.interfaces.RichGraphProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Special type of {@link GraphElement} containing some kind of logic.
 * TAKE INTO ACCOUNT THE FOLLOWING LIMITATIONS WHEN CREATING SUCH PLUGINS !!!
 * <p>
 * Plugins cam be thought of as extension of Storage, as they are allowed to created their own <strong>KeyedStates</strong>
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
     * Is this Plugin currently listening to graph updates
     */
    public boolean listening = true;

    /**
     * Reference to {@link GraphRuntimeContext}
     */
    public transient GraphRuntimeContext runtimeContext;


    public Plugin() {
        this(null);
    }

    public Plugin(String id) {
        this.id = id;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public short getPart() {
        return getRuntimeContext().getCurrentPart();
    }


    @Override
    public void setRuntimeContext(RuntimeContext t) {
        runtimeContext = (GraphRuntimeContext) t;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphRuntimeContext getRuntimeContext() {
        return runtimeContext;
    }
}
