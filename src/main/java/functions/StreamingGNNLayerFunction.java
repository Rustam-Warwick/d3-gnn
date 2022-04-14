package functions;

import elements.GraphOp;
import iterations.IterationType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

import java.util.List;

public class StreamingGNNLayerFunction extends KeyedProcessFunction<String, GraphOp, GraphOp> implements GNNLayerFunction {
    final OutputTag<GraphOp> iterateOutput = new OutputTag<GraphOp>("iterate") {
    };
    final OutputTag<GraphOp> backwardOutput = new OutputTag<GraphOp>("backward") {
    };
    public BaseStorage storage;
    public short position;
    public short numLayers;
    public transient short currentPart;
    public transient TimerService timerService;
    public transient Collector<GraphOp> collector;
    public transient KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx;
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
    public short getNumLayers() {
        return numLayers;
    }

    @Override
    public void message(GraphOp op) {
        try {
            if (op.state == IterationType.BACKWARD) {
                ctx.output(backwardOutput, op);
            } else if (op.state == IterationType.ITERATE) {
                ctx.output(iterateOutput, op);
            } else {
                collector.collect(op);
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sideMessage(GraphOp op, OutputTag<GraphOp> outputTag) {
        ctx.output(outputTag, op);
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
    public TimerService getTimerService() {
        return timerService;
    }

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        this.currentPart = Short.parseShort(ctx.getCurrentKey());
        process(value);
    }
}
