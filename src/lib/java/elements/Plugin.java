package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import org.apache.flink.runtime.state.taskshared.TaskSharedPluginMap;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphListener;
import org.apache.flink.streaming.api.operators.graph.interfaces.RichGraphProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Special type of {@link GraphElement} containing some kind of logic.
 * TAKE INTO ACCOUNT THE FOLLOWING LIMITATIONS WHEN CREATING SUCH PLUGINS !!!
 * <p>
 * It always lives in the <strong>{@link TaskSharedPluginMap}</strong>
 * Furthermore, Plugins cam be thought of as extension of Storage, as they are allowed to created their own <strong>KeyedStates</strong>
 * Plugins are typically stateless, otherwise thread-safe, since they can be accessed through multiple task threads and keys at once
 * It can have state objects but make sure to have them wrapped around {@link ThreadLocal} or get through {@link org.apache.flink.runtime.state.KeyedStateBackend}
 * Hence any {@link org.apache.flink.runtime.state.KeyedStateBackend} state access should go through ThreadLocal
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
