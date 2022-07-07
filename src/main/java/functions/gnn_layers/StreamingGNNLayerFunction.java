package functions.gnn_layers;

import elements.GraphOp;
import elements.iterations.MessageCommunication;
import elements.iterations.MessageDirection;
import operators.BaseWrapperOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.jetbrains.annotations.NotNull;
import storage.BaseStorage;

/**
 * GNNLayerFunction that assumes batch is ready per each process element
 */
public class StreamingGNNLayerFunction extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> implements GNNLayerFunction {
    public BaseStorage storage;
    public transient Collector<GraphOp> collector;
    public transient KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx;
    public transient BaseWrapperOperator<?>.Context baseWrapperContext;

    public StreamingGNNLayerFunction(BaseStorage storage) {
        this.storage = storage;
        storage.layerFunction = this;
    }

    @Override
    public BaseWrapperOperator<?>.Context getWrapperContext() {
        return baseWrapperContext;
    }

    @Override
    public void setWrapperContext(BaseWrapperOperator<?>.Context context) {
        this.baseWrapperContext = context;
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
        op.setPartId(null);
        op.setMessageCommunication(MessageCommunication.BROADCAST);
        message(op, direction);
    }

    @Override
    public void message(GraphOp op, MessageDirection direction, @NotNull Long timestamp) {
        try {
            if (direction == MessageDirection.BACKWARD) {
                getWrapperContext().outputWithTimestamp(op, BaseWrapperOperator.BACKWARD_OUTPUT_TAG, timestamp);
            } else if (direction == MessageDirection.FORWARD) {
                getWrapperContext().outputWithTimestamp(op, timestamp);
            } else {
                getWrapperContext().outputWithTimestamp(op, BaseWrapperOperator.ITERATE_OUTPUT_TAG, timestamp);
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void broadcastMessage(GraphOp op, MessageDirection direction, @NotNull Long timestamp) {
        op.setPartId(null);
        op.setMessageCommunication(MessageCommunication.BROADCAST);
        message(op, direction, timestamp);
    }

    @Override
    public <OUT> void sideMessage(OUT op, @NotNull OutputTag<OUT> outputTag, @NotNull Long timestamp) {
        getWrapperContext().outputWithTimestamp(op, outputTag, timestamp);
    }

    @Override
    public <OUT> void sideBroadcastMessage(OUT op, OutputTag<OUT> outputTag) {
        throw new IllegalStateException("Side BroadCast Messages not implemented yet!");
    }

    @Override
    public <OUT> void sideMessage(OUT op, OutputTag<OUT> outputTag) {
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
    public Long currentTimestamp() {
        return ctx.timestamp();
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
    public void onTimer(long timestamp, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.OnTimerContext ctx, Collector<GraphOp> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        getStorage().onTimer(timestamp);
    }

    @Override
    public TimerService getTimerService() {
        return ctx.timerService();
    }

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        if (this.collector == null) this.collector = out;
        if (this.ctx == null) this.ctx = ctx;
        process(value);
    }


}
