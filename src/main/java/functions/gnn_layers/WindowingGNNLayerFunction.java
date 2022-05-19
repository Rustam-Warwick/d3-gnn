package functions.gnn_layers;

import elements.GraphOp;
import elements.iterations.MessageCommunication;
import elements.iterations.MessageDirection;
import operators.BaseWrapperOperator;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

/**
 * GNNLayerFunction for Window based operators
 */
public class WindowingGNNLayerFunction implements InternalWindowFunction<Iterable<GraphOp>, GraphOp, String, TimeWindow>, GNNLayerFunction {
    public BaseStorage storage;
    public transient short currentPart;
    public RuntimeContext runtimeContext;
    public transient Collector<GraphOp> collector;
    public transient InternalWindowContext ctx;
    public transient BaseWrapperOperator<?>.Context baseWrapperContext;

    public WindowingGNNLayerFunction(BaseStorage storage) {
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
    public BaseStorage getStorage() {
        return storage;
    }

    @Override
    public void setStorage(BaseStorage storage) {
        this.storage = storage;
    }

    @Override
    public BaseWrapperOperator<?>.Context getWrapperContext() {
        return baseWrapperContext;
    }

    @Override
    public void setWrapperContext(BaseWrapperOperator<?>.Context context) {
        baseWrapperContext = context;
    }

    @Override
    public TimerService getTimerService() {
        return null;
    }

    @Override
    public long currentTimestamp() {
        return 0;
    }

    @Override
    public void message(GraphOp op, MessageDirection direction) {
        try {
            if (direction == MessageDirection.BACKWARD) {
                ctx.output(BaseWrapperOperator.BACKWARD_OUTPUT_TAG, op);
            } else if (direction == MessageDirection.FORWARD) {
                collector.collect(op);
            } else {
                ctx.output(BaseWrapperOperator.ITERATE_OUTPUT_TAG, op);
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void broadcastMessage(GraphOp op, MessageDirection direction) {
        op.setMessageCommunication(MessageCommunication.BROADCAST);
        try {
            if (direction == MessageDirection.BACKWARD) {
                getWrapperContext().broadcastElement(BaseWrapperOperator.BACKWARD_OUTPUT_TAG, op);
            } else if (direction == MessageDirection.FORWARD) {
                getWrapperContext().broadcastElement(null, op);
            } else {
                getWrapperContext().broadcastElement(BaseWrapperOperator.ITERATE_OUTPUT_TAG, op);
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <OUT> void sideBroadcastMessage(OUT op, OutputTag<OUT> outputTag) {
        getWrapperContext().broadcastElement(outputTag, op);
    }

    @Override
    public <OUT> void sideMessage(OUT op, OutputTag<OUT> outputTag) {
        ctx.output(outputTag, op);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        storage.open();
    }

    @Override
    public void close() throws Exception {
        storage.close();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return null;
    }

    @Override
    public void process(String s, TimeWindow window, InternalWindowContext context, Iterable<GraphOp> input, Collector<GraphOp> out) throws Exception {
        this.ctx = context;
        this.collector = out;
        setCurrentPart(Short.parseShort(s));
        input.forEach(this::process);
        getStorage().batch();
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        runtimeContext = t;
    }

    @Override
    public void clear(TimeWindow window, InternalWindowContext context) throws Exception {
    }

}
