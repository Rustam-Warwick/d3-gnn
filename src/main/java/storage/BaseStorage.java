package storage;

import elements.*;
import helpers.TaskNDManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

abstract public class BaseStorage extends KeyedProcessFunction<String, GraphOp, GraphOp> implements CheckpointedFunction{
    public short currentKey = 0; // Current Key being processed
    public short operatorIndex = 0; // Parallel Index of this operator
    public short maxParallelism = 1; // maxParallelism of this operator, also means max partitioning keys
    public short parallelism = 1; // actual parallelism
    public short position = 1; // Horizontal positiion of this operator
    public short layers = 1; // max horizontal number of GNN layers excluding the output layer

    // DATA FROM Actual Flink Operator
    public transient Collector<GraphOp> out;
    public transient KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx;
    public transient List<Short> thisKeys; // keys that get mapped to this operator
    public transient List<Short> otherKeys; // Keys of other operators
    public transient short thisMaster; // Master key of this subtask
    // End of Data From Actual Flink Operator

    public final HashMap<String, Plugin> plugins = new HashMap<>(); // Plugins

    public transient TaskNDManager manager; // Task ND Manager LifeCycle and per iteration manager

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
    public abstract void process(GraphOp element);



    public boolean isLast(){
        return this.position >= this.layers;
    }
    public boolean isFirst(){
        return this.position == 1;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if(out==null)return;
        super.open(parameters);
        this.manager = new TaskNDManager();
        this.parallelism = (short) getRuntimeContext().getNumberOfParallelSubtasks();
        this.maxParallelism = (short) getRuntimeContext().getMaxNumberOfParallelSubtasks();
        this.operatorIndex = (short) getRuntimeContext().getIndexOfThisSubtask();
        this.plugins.values().forEach(plugin->plugin.setStorage(this));
        this.plugins.values().forEach(Plugin::open);
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.plugins.values().forEach(Plugin::close);
        this.manager.close();
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, GraphOp, GraphOp>.OnTimerContext ctx, Collector<GraphOp> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        this.plugins.values().forEach(plugin->plugin.onTimer(timestamp, ctx, out));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    public void message(GraphOp msg){
        out.collect(msg);
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

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        this.currentKey = Short.parseShort(ctx.getCurrentKey());
        try{
            process(value);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            manager.clean();
        }
    }
}
