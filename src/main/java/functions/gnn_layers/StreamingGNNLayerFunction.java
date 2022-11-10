package functions.gnn_layers;

import elements.GraphElement;
import elements.GraphOp;
import elements.Rmi;
import elements.SyncElement;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import elements.enums.ReplicaState;
import operators.BaseWrapperOperator;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OutputTag;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storage.BaseStorage;
import typeinfo.byteinfo.ByteEnumTypeInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * GNNLayerFunction that also handles late sync message events
 */
public class StreamingGNNLayerFunction extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> implements GNNLayerFunction {

    public BaseStorage storage;

    public transient Collector<GraphOp> collector;

    public transient KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx;

    public transient BaseWrapperOperator<?>.Context baseWrapperContext;

    public transient MapState<Tuple2<String, ElementType>, List<Short>> lateSyncMessages;

    public transient Tuple2<String, ElementType> reuse;

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
        lateSyncMessages = getRuntimeContext().getMapState(
                new MapStateDescriptor<Tuple2<String, ElementType>, List<Short>>("lateSyncMessages",
                        Types.TUPLE(Types.STRING, new ByteEnumTypeInfo<>(ElementType.class)),
                        Types.LIST(Types.SHORT)));
        reuse = new Tuple2<>();
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

    @Override
    public void process(GraphOp value) {
        try {
            switch (value.op) {
                case COMMIT:
                    value.element.setStorage(getStorage());
                    if (!getStorage().containsElement(value.element)) {
                        value.element.create();
                        // See if there were some early sync messages that need to be replicated to
//                        if(value.element.elementType() != ElementType.ATTACHED_FEATURE
//                                && value.element.isReplicable()
//                                && !value.element.isHalo()){
//                            reuse.f0 = value.element.getId();
//                            reuse.f1 = value.element.elementType();
//                            if(lateSyncMessages.contains(reuse)){
//                                List<Short> replicaParts = lateSyncMessages.get(reuse);
//                                SyncElement syncElement = new SyncElement(reuse.f0, reuse.f1);
//                                replicaParts.forEach(item -> {
//                                    syncElement.partId = item;
//                                    value.element.sync(syncElement);
//                                });
//                                lateSyncMessages.remove(reuse);
//                            }
//                        }

                    } else {
                        GraphElement thisElement = getStorage().getElement(value.element);
                        thisElement.update(value.element);
                    }
                    break;
                case SYNC:
                    if (!getStorage().containsElement(value.element.getId(), value.element.elementType())) {
                        if(value.element.state() == ReplicaState.MASTER){
                            // Master -> Replica simply create the element
                            value.element.setStorage(getStorage());
                            value.element.create();
                        }else{
                            // Late event can only happen during Replica -> Master sync
                            SyncElement syncElement = (SyncElement) value.element;
                            List<Short> replicas;
                            if(lateSyncMessages.contains(syncElement.identity)) replicas = lateSyncMessages.get(syncElement.identity);
                            else replicas = new ArrayList<>(3);
                            replicas.add(syncElement.getPartId());
                            lateSyncMessages.put(syncElement.identity, replicas);
                        }
                    } else {
                        GraphElement el = getStorage().getElement(value.element.getId(), value.element.elementType());
                        el.sync(value.element);
                    }
                    break;
                case RMI:
                    GraphElement rpcElement = getStorage().getElement(value.element.getId(), value.element.elementType());
                    Rmi.execute(rpcElement, (Rmi) value.element);
                    break;
                case OPERATOR_EVENT:
                    getStorage().onOperatorEvent(value.getOperatorEvent());
                    break;
            }
        } catch (Exception | Error e) {
            BaseWrapperOperator.LOG.error(ExceptionUtils.stringifyException(e), value);
        }
    }

    }
