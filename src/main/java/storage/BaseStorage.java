package storage;

import elements.*;
import functions.gnn_layers.GNNLayerFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.stream.Stream;

/**
 * Base Class for all storage Engines
 *
 * @implNote Subclassses extendind from this class should not care about foreign keys. All GraphElements should be stored rather independent from each other
 * @implNote This is done so that late events are handled correctly, so all the logic is withing the specific graph element
 * @implNote However, do check for redundancy is create methods.
 */
abstract public class BaseStorage implements CheckpointedFunction, Serializable {
    /**
     * List of plugins attached to this storage engine
     * These are stored separately in operator state store
     */
    public final HashMap<String, Plugin> plugins = new HashMap<>();
    /**
     * The function that this BaseStorage is attached to
     */
    public GNNLayerFunction layerFunction;
    private transient ListState<HashMap<String, Plugin>> pluginListState;

    // -------- Abstract methods

    public abstract boolean addFeature(Feature feature);

    public abstract boolean addVertex(Vertex vertex);

    public abstract boolean addEdge(Edge edge);

    public abstract boolean updateFeature(Feature feature);

    public abstract boolean updateVertex(Vertex vertex);

    public abstract boolean updateEdge(Edge edge);

    public abstract boolean deleteFeature(Feature feature);

    public abstract boolean deleteVertex(Vertex vertex);

    public abstract boolean deleteEdge(Edge edge);

    public abstract Vertex getVertex(String id);

    public abstract Iterable<Vertex> getVertices();

    public abstract Edge getEdge(String id);

    public abstract Iterable<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type);

    public abstract Feature<?, ?> getFeature(String id);

    public abstract void cacheFeaturesOf(GraphElement e);

    // ----- Plugin Implementation and some common methods & CAllbacks

    public Plugin getPlugin(String id) {
        return this.plugins.get(id);
    }

    public Stream<Plugin> getPlugins() {
        return this.plugins.values().stream();
    }

    public BaseStorage withPlugin(Plugin plugin) {
        plugins.put(plugin.getId(), plugin);
        plugin.storage = this;
        plugin.add();
        return this;
    }

    /**
     * Operator opened
     */
    public void open() throws Exception {
        this.plugins.values().forEach(plugin -> plugin.setStorage(this));
        this.plugins.values().forEach(Plugin::open);
    }

    /**
     * Operator Closed
     */
    public void close() throws Exception {
        this.plugins.values().forEach(Plugin::close);
    }

    /**
     * OnTimer callback
     */
    public void onTimer(long timestamp) {
        plugins.values().forEach(plugin -> plugin.onTimer(timestamp));
    }

    /**
     * On OperatorEvent
     */
    public void onOperatorEvent(OperatorEvent event) {
        plugins.values().forEach(plugin -> {
            plugin.onOperatorEvent(event);
        });
    }


    // Operator State Handler
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // pass
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // pass
        ListStateDescriptor<HashMap<String, Plugin>> descriptor =
                new ListStateDescriptor(
                        "plugins",
                        TypeInformation.of(HashMap.class));
        pluginListState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            plugins.clear();
            pluginListState.get().forEach(plugins::putAll);
        } else {
            pluginListState.add(plugins);
        }

    }


    // Helper methods for creating and getting GraphElements
    public boolean addElement(GraphElement element) {
        switch (element.elementType()) {
            case VERTEX:
                return this.addVertex((Vertex) element);
            case EDGE:
                return this.addEdge((Edge) element);
            case FEATURE:
                return this.addFeature((Feature<?, ?>) element);
            default:
                return false;
        }
    }

    public boolean deleteElement(GraphElement element) {
        switch (element.elementType()) {
            case VERTEX:
                return this.deleteVertex((Vertex) element);
            case EDGE:
                return this.deleteEdge((Edge) element);
            case FEATURE:
                return this.deleteFeature((Feature<?, ?>) element);
            default:
                return false;
        }
    }

    public boolean updateElement(GraphElement element) {
        switch (element.elementType()) {
            case VERTEX:
                return this.updateVertex((Vertex) element);
            case EDGE:
                return this.updateEdge((Edge) element);
            case FEATURE:
                return this.updateFeature((Feature) element);
            default:
                return false;
        }
    }

    public GraphElement getElement(GraphElement element) {
        return this.getElement(element.getId(), element.elementType());
    }

    public GraphElement getElement(String id, ElementType t) {
        switch (t) {
            case VERTEX:
                return this.getVertex(id);
            case FEATURE:
                return this.getFeature(id);
            case EDGE:
                return this.getEdge(id);
            case PLUGIN:
                return this.getPlugin(id);
            default:
                return null;
        }
    }

}
