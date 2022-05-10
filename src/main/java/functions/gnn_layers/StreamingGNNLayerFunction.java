package functions.gnn_layers;

import elements.GraphOp;
import iterations.MessageDirection;
import operators.BaseWrapperOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

public class StreamingGNNLayerFunction extends KeyedProcessFunction<String, GraphOp, GraphOp> implements GNNLayerFunction {
    public BaseStorage storage;
    public short position;
    public short numLayers;
    public transient short currentPart;
    public transient Collector<GraphOp> collector;
    public transient KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx;
    public transient BaseWrapperOperator.Context baseWrapperContext;

    public StreamingGNNLayerFunction(BaseStorage storage, short position, short numLayers) {
        this.position = position;
        this.numLayers = numLayers;
        this.storage = storage;
        storage.layerFunction = this;
    }

    @Override
    public short getCurrentPart() {
        return currentPart;
    }

    @Override
    public void setCurrentPart(short part) {
        this.currentPart = part;
    }

    @Override
    public short getPosition() {
        return position;
    }

    @Override
    public void setPosition(short position) {
        this.position = position;
    }

    @Override
    public BaseWrapperOperator.Context getWrapperContext() {
        return baseWrapperContext;
    }

    @Override
    public void setWrapperContext(BaseWrapperOperator.Context context) {
        this.baseWrapperContext = context;
    }

    @Override
    public short getNumLayers() {
        return numLayers;
    }

    @Override
    public void setNumLayers(short numLayers) {
        this.numLayers = numLayers;
    }

    @Override
    public void message(GraphOp op, MessageDirection direction) {
        try {
            if (direction == MessageDirection.BACKWARD) {
                ctx.output(BaseWrapperOperator.backwardOutputTag, op);
            } else if (direction == MessageDirection.FORWARD) {
                collector.collect(op);
            } else {
                ctx.output(BaseWrapperOperator.iterateOutputTag, op);
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
    public BaseStorage getStorage() {
        return storage;
    }

    @Override
    public void setStorage(BaseStorage storage) {
        this.storage = storage;
    }

    @Override
    public long currentTimestamp() {
        return ctx.timestamp() == null ? 0 : ctx.timestamp();
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
    public void onTimer(long timestamp, KeyedProcessFunction<String, GraphOp, GraphOp>.OnTimerContext ctx, Collector<GraphOp> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        getStorage().onTimer(timestamp);
    }

    @Override
    public TimerService getTimerService() {
        return ctx.timerService();
    }

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        setCurrentPart(Short.parseShort(ctx.getCurrentKey()));
        if (this.collector == null) this.collector = out;
        if (this.ctx == null) this.ctx = ctx;
        process(value);
    }
}
