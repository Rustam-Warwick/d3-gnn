package storage;

import elements.*;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import functions.gnn_layers.GNNLayerFunction;
import operators.events.BaseOperatorEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

/**
 * Base Class for all storage Engines
 *
 * @implNote Subclassses extendind from this class should not care about foreign keys. All GraphElements should be stored rather independent from each other
 * @implNote This is done so that late events are handled correctly, so all the logic is withing the specific graph element
 * @implNote However, do check for redundancy is create methods.
 */
abstract public class BaseStorage implements CheckpointedFunction, Serializable {
    protected static Logger LOG = LoggerFactory.getLogger(BaseStorage.class);

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
     * Late events addressed to element that is not here yet
     */
    public transient HashMap<Tuple2<String, ElementType>, List<GraphOp>> lateEvents;
    /**
     * KeySelector change listener
     */
    private transient RemoveCachedFeatures removeCachedFeatures;

    // ------------------------ ABSTRACT METHODS -------------------------------------

    public abstract boolean addAttachedFeature(Feature<?, ?> feature);

    public abstract boolean addStandaloneFeature(Feature<?, ?> feature);

    public abstract boolean addVertex(Vertex vertex);

    public abstract boolean addEdge(DEdge dEdge);

    public abstract boolean addHyperEdge(HEdge hEdge);

    public abstract boolean updateAttachedFeature(Feature<?, ?> feature, Feature<?, ?> memento);

    public abstract boolean updateStandaloneFeature(Feature<?, ?> feature, Feature<?, ?> memento);

    public abstract boolean updateVertex(Vertex vertex, Vertex memento);

    public abstract boolean updateEdge(DEdge dEdge, DEdge memento);

    public abstract boolean updateHyperEdge(HEdge hEdge, HEdge memento);

    public abstract boolean deleteAttachedFeature(Feature<?, ?> feature);

    public abstract boolean deleteStandaloneFeature(Feature<?, ?> feature);

    public abstract boolean deleteVertex(Vertex vertex);

    public abstract boolean deleteEdge(DEdge dEdge);

    public abstract boolean deleteHyperEdge(HEdge hEdge);

    @Nullable
    public abstract Vertex getVertex(String id);

    public abstract Iterable<Vertex> getVertices();

    @Nullable
    public abstract DEdge getEdge(String srcId, String destId, @Nullable String attributeId, @Nullable String id);

    public abstract Iterable<DEdge> getEdges(String src, String dest);

    public abstract Iterable<DEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type);

    public abstract HEdge getHyperEdge(String id);

    public abstract Iterable<HEdge> getIncidentHyperEdges(Vertex id);

    @Nullable
    public abstract Feature<?, ?> getAttachedFeature(String elementId, String featureName, ElementType elementType, @Nullable String id);

    @Nullable
    public abstract Feature<?, ?> getStandaloneFeature(String id);

    public abstract boolean containsVertex(String id);

    public abstract boolean containsAttachedFeature(String elementId, String featureName, ElementType elementType, @Nullable String id);

    public abstract boolean containsStandaloneFeature(String id);

    public abstract boolean containsEdge(String srcId, String destId, @Nullable String attributeId, @Nullable String id);

    public abstract boolean containsHyperEdge(String id);

    public abstract void cacheNonHaloFeatures(GraphElement element);

    // -------------------------- BASESTORAGE METHODS ------------------------------


    /**
     * Do elements need to delay Tensors on serialization
     */
    public boolean needsTensorDelay() {
        return true;
    }

    /**
     * Delay this event until later time
     */
    public final void delayEvent(String id, ElementType elementType, GraphOp op) {
        lateEvents.compute(Tuple2.of(id, elementType), (key, val) -> {
            if (val == null) val = new ArrayList<>(5);
            val.add(op);
            op.delay();
            return val;
        });
    }

    /**
     * Flush or delayed events
     */
    public final void flushDelayedEvents(GraphElement el) {
        Objects.<List<GraphOp>>requireNonNullElse(
                lateEvents.remove(Tuple2.of(el.getId(), el.elementType())),
                Collections.emptyList()
        ).forEach(op -> {
            layerFunction.process(op);
            op.resume();
        });
    }

    /**
     * Retrive plugin
     */
    public Plugin getPlugin(String id) {
        return this.plugins.get(id);
    }

    /**
     * Iterate over all plugins
     *
     * @return
     */
    public Iterable<Plugin> getPlugins() {
        return this.plugins.values();
    }

    /**
     * Register a callback to be fired in the future
     */
    public void runCallback(@Nullable Consumer<BaseStorage> a) {
        if (a != null) a.accept(this);
    }

    public BaseStorage withPlugin(Plugin plugin) {
        assert plugin.getId() != null;
        plugins.put(plugin.getId(), plugin);
        return this;
    }

    /**
     * Operator opened
     */
    public void open() throws Exception {
        removeCachedFeatures = new RemoveCachedFeatures();
        layerFunction.registerKeyChangeListener(removeCachedFeatures);
        lateEvents = new HashMap<>();
        plugins.values().forEach(plugin -> plugin.setStorage(this));
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
    public void onOperatorEvent(BaseOperatorEvent event) {
        for (Plugin value : plugins.values()) {
            value.onOperatorEvent(event);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        for (Plugin value : plugins.values()) {
            value.snapshotState(context);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        for (Plugin value : plugins.values()) {
            value.initializeState(context);
        }
    }


    // --------------------------- MAPPER & HELPER METHODS -------------------------

    public boolean addElement(GraphElement element) {
        switch (element.elementType()) {
            case VERTEX:
                return this.addVertex((Vertex) element);
            case EDGE:
                return this.addEdge((DEdge) element);
            case ATTACHED_FEATURE:
                return addAttachedFeature((Feature<?, ?>) element);
            case STANDALONE_FEATURE:
                return addStandaloneFeature((Feature<?, ?>) element);
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
                return this.deleteEdge((DEdge) element);
            case ATTACHED_FEATURE:
                return this.deleteAttachedFeature((Feature<?, ?>) element);
            case STANDALONE_FEATURE:
                return this.deleteStandaloneFeature((Feature<?, ?>) element);
            case HYPEREDGE:
                return this.deleteHyperEdge((HEdge) element);
            default:
                return false;
        }
    }

    public boolean updateElement(GraphElement element, GraphElement memento) {
        switch (element.elementType()) {
            case VERTEX:
                return this.updateVertex((Vertex) element, (Vertex) memento);
            case EDGE:
                return this.updateEdge((DEdge) element, (DEdge) element);
            case ATTACHED_FEATURE:
                return this.updateAttachedFeature((Feature<?, ?>) element, (Feature<?, ?>) memento);
            case STANDALONE_FEATURE:
                return this.updateStandaloneFeature((Feature<?, ?>) element, (Feature<?, ?>) memento);
            case HYPEREDGE:
                return this.updateHyperEdge((HEdge) element, (HEdge) memento);
            default:
                return false;
        }
    }

    public boolean containsElement(String id, ElementType type) {
        switch (type) {
            case VERTEX:
                return containsVertex(id);
            case EDGE:
                Tuple3<String, String, String> ids = DEdge.decodeVertexIdsAndAttribute(id);
                return containsEdge(ids.f0, ids.f1, ids.f2, id);
            case ATTACHED_FEATURE:
                Tuple3<ElementType, String, String> tmp = Feature.decodeAttachedFeatureId(id);
                return containsAttachedFeature(tmp.f1, tmp.f2, tmp.f0, id);
            case STANDALONE_FEATURE:
                return containsStandaloneFeature(id);
            case PLUGIN:
                return plugins.containsKey(id);
            case HYPEREDGE:
                return containsHyperEdge(id);
            default:
                return false;
        }
    }

    public GraphElement getElement(String id, ElementType t) {
        switch (t) {
            case VERTEX:
                return this.getVertex(id);
            case ATTACHED_FEATURE:
                Tuple3<ElementType, String, String> tmp = Feature.decodeAttachedFeatureId(id);
                return getAttachedFeature(tmp.f1, tmp.f2, tmp.f0, id);
            case STANDALONE_FEATURE:
                return getStandaloneFeature(id);
            case EDGE:
                Tuple3<String, String, String> ids = DEdge.decodeVertexIdsAndAttribute(id);
                return getEdge(ids.f0, ids.f1, ids.f2, id);
            case PLUGIN:
                return getPlugin(id);
            case HYPEREDGE:
                return getHyperEdge(id);
            default:
                return null;
        }
    }

    public boolean containsElement(GraphElement element) {
        switch (element.elementType()) {
            case VERTEX:
                return containsVertex(element.getId());
            case ATTACHED_FEATURE:
                Feature<?, ?> tmp = (Feature<?, ?>) element;
                return containsAttachedFeature(tmp.attachedTo.f1, tmp.attachedTo.f2, tmp.attachedTo.f0, null);
            case STANDALONE_FEATURE:
                return containsStandaloneFeature(element.getId());
            case EDGE:
                DEdge edge = (DEdge) element;
                return containsEdge(edge.getSrcId(), edge.getDestId(), edge.getAttribute(), null);
            case PLUGIN:
                return plugins.containsKey(element.getId());
            case HYPEREDGE:
                return containsHyperEdge(element.getId());
            default:
                return false;
        }
    }

    public GraphElement getElement(GraphElement element) {
        switch (element.elementType()) {
            case VERTEX:
                return getVertex(element.getId());
            case ATTACHED_FEATURE:
                Feature<?, ?> tmp = (Feature<?, ?>) element;
                return getAttachedFeature(tmp.attachedTo.f1, tmp.attachedTo.f2, tmp.attachedTo.f0, null);
            case STANDALONE_FEATURE:
                return getStandaloneFeature(element.getId());
            case EDGE:
                DEdge edge = (DEdge) element;
                return getEdge(edge.getSrcId(), edge.getDestId(), edge.getAttribute(), null);
            case PLUGIN:
                return getPlugin(element.getId());
            case HYPEREDGE:
                return getHyperEdge(element.getId());
            default:
                return null;
        }
    }

    // Remove Cached Plugin Features on Key Change. Important since plugins are always in memory
    private class RemoveCachedFeatures implements KeyedStateBackend.KeySelectionListener<Object> {
        @Override
        public void keySelected(Object newKey) {
            plugins.values().forEach(plugin -> {
                if (plugin.features != null) plugin.features.clear();
            });
        }
    }

}
