package functions.storage;

import elements.GraphOp;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import operators.BaseWrapperOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storage.BaseStorage;

/**
 * GNNLayerFunction that also handles late sync message events
 */
public class StreamingStorageProcessFunction extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> implements StorageProcessFunction {

    public BaseStorage storage;

    public transient Collector<GraphOp> collector;

    public transient KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx;

    public transient BaseWrapperOperator<?>.Context baseWrapperContext;

    public transient Tuple2<String, ElementType> reuse;

    public StreamingStorageProcessFunction(BaseStorage storage) {
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
        if (direction == MessageDirection.BACKWARD) {
            ctx.output(BaseWrapperOperator.BACKWARD_OUTPUT_TAG, op);
        } else if (direction == MessageDirection.FORWARD) {
            collector.collect(op);
        } else {
            ctx.output(BaseWrapperOperator.ITERATE_OUTPUT_TAG, op);
        }
    }

    @Override
    public void message(GraphOp op, MessageDirection direction, @NotNull Long timestamp) {
        if (direction == MessageDirection.BACKWARD) {
            getWrapperContext().output(op, BaseWrapperOperator.BACKWARD_OUTPUT_TAG, timestamp);
        } else if (direction == MessageDirection.FORWARD) {
            getWrapperContext().output(op, null, timestamp);
        } else {
            getWrapperContext().output(op, BaseWrapperOperator.ITERATE_OUTPUT_TAG, timestamp);
        }
    }

    @Override
    public void broadcastMessage(GraphOp op, MessageDirection direction) {
        if (direction == MessageDirection.BACKWARD) {
            getWrapperContext().broadcastOutput(op, BaseWrapperOperator.BACKWARD_OUTPUT_TAG, null);
        } else if (direction == MessageDirection.FORWARD) {
            getWrapperContext().broadcastOutput(op, null, null);
        } else {
            getWrapperContext().broadcastOutput(op, BaseWrapperOperator.ITERATE_OUTPUT_TAG, null);
        }
    }

    @Override
    public void broadcastMessage(GraphOp op, MessageDirection direction, @Nullable Long timestamp) {
        if (direction == MessageDirection.BACKWARD) {
            getWrapperContext().broadcastOutput(op, BaseWrapperOperator.BACKWARD_OUTPUT_TAG, timestamp);
        } else if (direction == MessageDirection.FORWARD) {
            getWrapperContext().broadcastOutput(op, null, timestamp);
        } else {
            getWrapperContext().broadcastOutput(op, BaseWrapperOperator.ITERATE_OUTPUT_TAG, timestamp);
        }
    }

    @Override
    public <OUT> void sideMessage(OUT op, @NotNull OutputTag<OUT> outputTag, @NotNull Long timestamp) {
        getWrapperContext().output(op, outputTag, timestamp);
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
