package storage;

import ai.djl.ndarray.NDManager;
import elements.*;
import helpers.TaskNDManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.OnWatermarkCallback;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import state.KeyGroupRangeAssignment;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

abstract public class BaseStorage extends KeyedProcessFunction<String, GraphOp, GraphOp> implements CheckpointedFunction{
    public short currentKey = 0; // Current Key being processes
    public short operatorIndex = 0; // Index of this operator
    public short maxParallelism = 1; // maxParallelism of this operator, also means max partitioning
    public short parallelism = 1; // actual parallelism
    public short position = 1; // horizontal positiion of this operator
    public short layers = 1; // max horizontal number of GNN layers excluding the output layer

    public final HashMap<String, Plugin> plugins = new HashMap<>(); // Plugins
    public final List<Short> keys = new ArrayList<>(); // keys that get mapped to this operator

    public transient TaskNDManager manager; // Task ND Manager LifeCycle and per iteration manager

    // Abstract Functions

    public abstract boolean addFeature(Feature feature);
    public abstract boolean addVertex(Vertex vertex);
    public abstract boolean addEdge(Edge edge);
    public abstract boolean updateFeature(Feature feature);
    public abstract boolean updateVertex(Vertex vertex);
    public abstract boolean updateEdge(Edge edge);
    public abstract Vertex getVertex(String id);
    public abstract Iterable<Vertex> getVertices();
    public abstract Edge getEdge(String id);
    public abstract Iterable<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type);
    public abstract Feature getFeature(String id);
    public abstract Map<String, Feature> getFeaturesOf(GraphElement e);

    public abstract void message(GraphOp op);
    public boolean isLast(){
        return this.position >= this.layers;
    }
    public boolean isFirst(){
        return this.position == 1;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.manager = new TaskNDManager();
        this.parallelism = (short) getRuntimeContext().getNumberOfParallelSubtasks();
        this.maxParallelism = (short) getRuntimeContext().getMaxNumberOfParallelSubtasks();
        this.operatorIndex = (short) getRuntimeContext().getIndexOfThisSubtask();
        findAllKeys(); // Find all keys of this operator
        this.plugins.values().forEach(plugin->plugin.setStorage(this));
        this.plugins.values().forEach(Plugin::open);
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.manager.close();
        this.plugins.values().forEach(Plugin::close);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, GraphOp, GraphOp>.OnTimerContext ctx, Collector<GraphOp> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        this.plugins.values().forEach(plugin->plugin.onTimer(timestamp, ctx, out));
    }

    public void onWatermark(Watermark watermark){
        System.out.println("Watermark on index: " + operatorIndex+" Position: "+ position);
//        this.plugins.values().forEach(plugin->plugin.onWatermark(watermark));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    private void findAllKeys(){
        for(short i=0;i<maxParallelism;i++){
            int resultingIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(String.valueOf(i), maxParallelism, parallelism);
            if(resultingIndex == operatorIndex){
                keys.add(i);
            }
        }
    }

    public boolean addElement(GraphElement element){
        switch (element.elementType()){
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

    public boolean updateElement(GraphElement element){
        switch (element.elementType()){
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

    public GraphElement getElement(GraphElement element){
        return this.getElement(element.getId(), element.elementType());
    }

    public GraphElement getElement(String id, ElementType t){
        switch (t){
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

    /**
     * Adds Plugins to storage engine, this will be recorded in the state after open()
     * @param plugin Plugin to be added to the storage engine
     * @return
     */
    public BaseStorage withPlugin(Plugin plugin) {
        this.plugins.put(plugin.getId(), plugin);
        plugin.setStorage(this);
        plugin.add();
        return this;
    }

    public Plugin getPlugin(String id){
        return this.plugins.get(id);
    }

    public Stream<Plugin> getPlugins(){
        return this.plugins.values().stream();
    }

}
