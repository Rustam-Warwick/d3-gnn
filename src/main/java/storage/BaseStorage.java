package storage;

import elements.*;
import functions.gnn_layers.GNNLayerFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.function.Consumer;

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

    /**
     * Plugins in the list state
     */
    private transient ListState<HashMap<String, Plugin>> pluginListState; // Plus stored in operator state

    private transient RemoveCachedFeatures removeCachedFeatures;

    // -------- Abstract methods

    // -- Add
    public abstract boolean addFeature(Feature<?, ?> feature);

    public abstract boolean addVertex(Vertex vertex);

    public abstract boolean addEdge(Edge edge);

    public abstract boolean addHyperEdge(HEdge hEdge);

    // -- Update
    public abstract boolean updateFeature(Feature<?, ?> feature);

    public abstract boolean updateVertex(Vertex vertex);

    public abstract boolean updateEdge(Edge edge);

    public abstract boolean updateHyperEdge(HEdge hEdge);

    // -- Delete
    public abstract boolean deleteFeature(Feature<?, ?> feature);

    public abstract boolean deleteVertex(Vertex vertex);

    public abstract boolean deleteEdge(Edge edge);

    public abstract boolean deleteHyperEdge(HEdge hEdge);

    // -- Get
    // - Vertex
    @Nullable
    public abstract Vertex getVertex(String id);

    public abstract Iterable<Vertex> getVertices();

    // - Edge
    @Nullable
    public abstract Edge getEdge(String id);

    public abstract Iterable<Edge> getEdges(String src, String dest); // Possible-multigraph

    public abstract Iterable<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type);

    // - HyperEdge
    public abstract HEdge getHyperEdge(String id);

    public abstract Iterable<HEdge> getHyperEdges(Vertex id);

    // - Feature
    @Nullable
    public abstract Feature<?, ?> getFeature(String id);

    // -- Contains
    public abstract boolean containsVertex(String id);

    public abstract boolean containsFeature(String id);

    public abstract boolean containsEdge(String id);

    public abstract boolean containsHyperEdge(String id);

    public abstract void cacheFeaturesOf(GraphElement e);


    // ----- Plugin Implementation and some common methods & CAllbacks

    public Plugin getPlugin(String id) {
        return this.plugins.get(id);
    }

    protected Iterable<Plugin> getPlugins() {
        return this.plugins.values();
    }

    /**
     * Register a callback to be fired in the future
     */
    public void runCallback(Consumer<Plugin> a){
        for (Plugin value : plugins.values()) {
            a.accept(value);
        }
    }

    public BaseStorage withPlugin(Plugin plugin) {
        assert plugin.getId() != null;
        plugins.put(plugin.getId(), plugin);
        plugin.storage = this;
        plugin.add();
        return this;
    }

    /**
     * Operator opened
     */
    public void open() throws Exception {
        removeCachedFeatures = new RemoveCachedFeatures();
        layerFunction.registerKeyChangeListener(removeCachedFeatures);
        this.plugins.values().forEach(plugin -> plugin.setStorage(this));
        for (Plugin value : plugins.values()) {
            value.open();
        }
    }

    /**
     * Operator Closed
     */
    public void close() throws Exception {
        for (Plugin value : plugins.values()) {
            value.close();
        }
        layerFunction.deRegisterKeyChangeListener(removeCachedFeatures);
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
        for (Plugin value : plugins.values()) {
            value.onOperatorEvent(event);
        }
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
                        TypeInformation.of(new TypeHint<HashMap<String, Plugin>>() {
                        }));
        pluginListState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            plugins.clear();
            pluginListState.get().forEach(plugins::putAll);
        } else {
            pluginListState.add(plugins);
        }

    }


    /**
     * Helper methods
     */
    public boolean addElement(GraphElement element) {
        switch (element.elementType()) {
            case VERTEX:
                return this.addVertex((Vertex) element);
            case EDGE:
                return this.addEdge((Edge) element);
            case FEATURE:
                return this.addFeature((Feature<?, ?>) element);
            case HYPEREDGE:
                return this.addHyperEdge((HEdge) element);
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
            case HYPEREDGE:
                return this.deleteHyperEdge((HEdge) element);
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
            case HYPEREDGE:
                return this.updateHyperEdge((HEdge) element);
            default:
                return false;
        }
    }

    public boolean containsElement(String id, ElementType type) {
        switch (type) {
            case VERTEX:
                return this.containsVertex(id);
            case EDGE:
                return this.containsEdge(id);
            case FEATURE:
                return this.containsFeature(id);
            case PLUGIN:
                return this.plugins.containsKey(id);
            case HYPEREDGE:
                return this.containsHyperEdge(id);
            default:
                return false;
        }
    }

    public boolean containsElement(GraphElement element) {
        return containsElement(element.getId(), element.elementType());
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
            case HYPEREDGE:
                return this.getHyperEdge(id);
            default:
                return null;
        }
    }

    private class RemoveCachedFeatures implements KeyedStateBackend.KeySelectionListener<Object> {
        @Override
        public void keySelected(Object newKey) {
            plugins.values().forEach(Plugin::clearFeatures);
        }
    }

}
