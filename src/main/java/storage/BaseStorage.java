package storage;

import elements.*;
import functions.gnn_layers.GNNLayerFunction;
import helpers.TaskNDManager;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.stream.Stream;

abstract public class BaseStorage implements CheckpointedFunction, Serializable {
    /**
     * List of plugins attached to this storage engine
     */
    public final HashMap<String, Plugin> plugins = new HashMap<>();
    public GNNLayerFunction layerFunction;
    /**
     * Helper manager for managing tensor memory
     */
    public transient TaskNDManager manager; // Task ND Manager LifeCycle and per iteration manager

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

    public abstract Iterable<Edge> getEdges();

    public abstract Feature getFeature(String id);

    public abstract void cacheFeaturesOf(GraphElement e);


    public Plugin getPlugin(String id) {
        return this.plugins.get(id);
    }

    public Stream<Plugin> getPlugins() {
        return this.plugins.values().stream();
    }

    public BaseStorage withPlugin(Plugin plugin) {
        this.plugins.put(plugin.getId(), plugin);
        plugin.storage = this;
        plugin.add();
        return this;
    }

    public void open() throws Exception {
        this.manager = new TaskNDManager();
        this.plugins.values().forEach(plugin -> plugin.setStorage(this));
        this.plugins.values().forEach(Plugin::open);
    }

    public void close() throws Exception {
        this.plugins.values().forEach(Plugin::close);
        this.manager.close();
    }

    public void onTimer(long timestamp) {
        plugins.values().forEach(plugin -> plugin.onTimer(timestamp));
    }

    public void onWatermark(long timestamp) {
        plugins.values().forEach(plugin -> plugin.onWatermark(timestamp));
    }

    public void onPreWatermark(long timestamp){
        plugins.values().forEach(plugin -> plugin.onPreWatermark(timestamp));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // pass
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // pass
    }

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
