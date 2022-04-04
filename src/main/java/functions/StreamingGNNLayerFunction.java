package functions;

import elements.GraphOp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import storage.BaseStorage;

import java.util.List;

public class StreamingGNNLayerFunction extends KeyedProcessFunction<String, GraphOp, GraphOp> implements GNNLayerFunction {
    public BaseStorage storage;
    public short currentPart;
    public short position;
    public short layers;
    public transient Collector<GraphOp> collector;
    public transient List<Short> thisParts;
    public transient List<Short> replicaMasterParts;
    public transient short masterPart;
    public StreamingGNNLayerFunction(BaseStorage storage) {
        this.storage = storage;
        storage.layerFunction = this;
    }

    @Override
    public short getCurrentPart() {
        return currentPart;
    }

    @Override
    public short getMasterPart() {
        return masterPart;
    }

    @Override
    public List<Short> getThisParts() {
        return thisParts;
    }

    @Override
    public List<Short> getReplicaMasterParts() {
        return replicaMasterParts;
    }

    @Override
    public short getPosition() {
        return position;
    }

    @Override
    public short getLayers() {
        return layers;
    }

    @Override
    public void message(GraphOp op) {
        collector.collect(op);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getStorage().open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        getStorage().close();
    }

    @Override
    public BaseStorage getStorage() {
        return storage;
    }

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        this.currentPart = Short.parseShort(ctx.getCurrentKey());
        process(value);

    }
}
