package org.apache.flink.streaming.runtime.tasks;

import elements.GraphOp;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.CountingOutput;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Special Counting Output Collector for {@link GraphOp}.
 * <ul>
 *     <li> Counting Output Metrics </li>
 *     <li> Handling broadcast & selective broadcast messages for output channels with graphOp types </li>
 *     <li> Methods for analysing the r-ty of the individual output gates </li>
 * </ul>
 *
 * @implNote Assumes the underlying serializer is {@link typeinfo.graphopinfo.GraphOpSerializer} joined with {@link org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer}
 * @implNote Broadcast and selective broadcast messages only work for {@link RecordWriterOutput} channels
 * @implNote Broadcast messages are only counted as a single record output @todo Fix later
 */
public class CountingBroadcastingGraphOutputCollector extends BroadcastingOutputCollector<GraphOp> {

    static Field outputTagField;

    static Field recordWriterField;

    static Field serializationDelegateField;

    static Field numChannelsField;

    static Field countingOutputOutputField;

    static {
        try {
            outputTagField = RecordWriterOutput.class.getDeclaredField("outputTag");
            recordWriterField = RecordWriterOutput.class.getDeclaredField("recordWriter");
            serializationDelegateField = RecordWriterOutput.class.getDeclaredField("serializationDelegate");
            numChannelsField = RecordWriter.class.getDeclaredField("numberOfChannels");
            countingOutputOutputField = CountingOutput.class.getDeclaredField("output");
            outputTagField.setAccessible(true);
            recordWriterField.setAccessible(true);
            serializationDelegateField.setAccessible(true);
            numChannelsField.setAccessible(true);
            countingOutputOutputField.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException("Could not initialization GraphOutputCollected, turn off Security manager");
        }
    }

    /**
     * Reference to the fields in the {@link RecordWriterOutput}
     */
    protected final Tuple4<OutputTag<GraphOp>, RecordWriter<SerializationDelegate<StreamElement>>, HelperSerializationDelegate, Integer>[] outputInternalInfo;

    public CountingBroadcastingGraphOutputCollector(Output<StreamRecord<GraphOp>> output, Counter numRecordsOutCounter) {
        super(extractOutputs(output), numRecordsOutCounter);
        this.outputInternalInfo = new Tuple4[outputs.length];
        try {
            for (int i = 0; i < outputs.length; i++) {
                if (outputs[i] instanceof RecordWriterOutput) {
                    OutputTag<?> tag = (OutputTag<?>) outputTagField.get(outputs[i]);
                    if (tag != null && !tag.getTypeInfo().getTypeClass().equals(GraphOp.class))
                        continue; // Not adding non-GraphOp output tags
                    RecordWriter<SerializationDelegate<StreamElement>> recordWriter = (RecordWriter<SerializationDelegate<StreamElement>>) recordWriterField.get(outputs[i]);
                    SerializationDelegate<StreamElement> serializationDelegateOld = (SerializationDelegate<StreamElement>) serializationDelegateField.get(outputs[i]);
                    HelperSerializationDelegate serializationDelegate = new HelperSerializationDelegate(serializationDelegateOld);
                    serializationDelegateField.set(outputs[i], serializationDelegate);
                    outputInternalInfo[i] = Tuple4.of((OutputTag<GraphOp>) tag, recordWriter, serializationDelegate, (Integer) numChannelsField.get(recordWriter));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not initialize GraphOutputCollected, turn off Security Manager");
        }
    }

    /**
     * Broadcast single message to all output channels in all connected edges
     */
    public void broadcastAll(StreamRecord<GraphOp> record) {
        for (Tuple4<OutputTag<GraphOp>, RecordWriter<SerializationDelegate<StreamElement>>, HelperSerializationDelegate, Integer> info : outputInternalInfo) {
            if (info == null) continue;
            info.f2.setInstance(record);
            try {
                info.f1.broadcastEmit(info.f2);
                numRecordsOutForTask.inc(info.f3);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }

        }
    }

    /**
     * Broadcast single message to all output channels for an {@link OutputTag} output or forward if {@code outputTag == null}
     */
    public void broadcast(@Nullable OutputTag<GraphOp> outputTag, StreamRecord<GraphOp> record) {
        for (Tuple4<OutputTag<GraphOp>, RecordWriter<SerializationDelegate<StreamElement>>, HelperSerializationDelegate, Integer> info : outputInternalInfo) {
            if (info == null || (outputTag == null ^ info.f0 == null) || (info.f0 != null && !OutputTag.isResponsibleFor(outputTag, info.f0)))
                continue;
            info.f2.setInstance(record);
            try {
                info.f1.broadcastEmit(info.f2);
                numRecordsOutForTask.inc(info.f3);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            return;
        }
        throw new IllegalStateException("No output responsible for broadcast message");
    }

    /**
     * Selectively broadcast {@link GraphOp} to all channels for {@link OutputTag} or forward if {@code outputTag == null}
     */
    public void broadcast(@Nullable OutputTag<GraphOp> outputTag, StreamRecord<GraphOp> record, List<Short> selectedParts) {
        for (int i = 0; i < outputInternalInfo.length; i++) {
            if (outputInternalInfo[i] == null || (outputTag == null ^ outputInternalInfo[i].f0 == null) || (outputInternalInfo[i].f0 != null && !OutputTag.isResponsibleFor(outputTag, outputInternalInfo[i].f0)))
                continue;
            outputInternalInfo[i].f2.broadcastStart();
            for (short selectedPart : selectedParts) {
                record.getValue().setPartId(selectedPart);
                outputs[i].collect(outputTag, record);
            }
            numRecordsOutForTask.inc(selectedParts.size());
            outputInternalInfo[i].f2.broadcastFinish();
            return;
        }
        throw new IllegalStateException("No output responsible for broadcast message");
    }

    /**
     * Gets the number of output channel in the given output tag
     */
    public int getNumChannels(@Nullable OutputTag<GraphOp> outputTag) {
        for (Tuple4<OutputTag<GraphOp>, RecordWriter<SerializationDelegate<StreamElement>>, HelperSerializationDelegate, Integer> info : outputInternalInfo) {
            if (info == null || (outputTag == null ^ info.f0 == null) || (info.f0 != null && !OutputTag.isResponsibleFor(outputTag, info.f0)))
                continue;
            return info.f3;
        }
        throw new IllegalStateException("No output responsible for broadcast message");
    }

    /**
     * Wrapper around {@link RecordWriterOutput}'s {@link SerializationDelegate} that
     * Helps to handle single serialization for multiple part outputs.
     */
    static class HelperSerializationDelegate extends SerializationDelegate<StreamElement> {

        /**
         * Main {@link SerializationDelegate}, everything is normally delegated to this guy
         */
        final SerializationDelegate<StreamElement> mainSerializationDelegate;

        /**
         * <p>
         * 0 -> Normal mode of execution, need to delegate everything to mainSerializationDelegate
         * 1 -> Started broadcasting but haven't yet received the first element of broadcast
         * >1 -> Broadcasting and the out already holds the serialized value and this int is the capacity of the serialized buffer
         * </p>
         */
        int broadcastStarted = 0;

        public HelperSerializationDelegate(SerializationDelegate<StreamElement> mainSerializationDelegate) {
            super(null);
            this.mainSerializationDelegate = mainSerializationDelegate;
        }

        @Override
        public StreamElement getInstance() {
            return mainSerializationDelegate.getInstance();
        }

        @Override
        public void setInstance(StreamElement instance) {
            mainSerializationDelegate.setInstance(instance);
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            switch (broadcastStarted) {
                case 0:
                    // Individual serialization
                    mainSerializationDelegate.write(out);
                    break;
                case 1:
                    // Start of broadcast serialization, serialize once and then re-use the remaining ones
                    mainSerializationDelegate.write(out);
                    broadcastStarted = ((DataOutputSerializer) out).length();
                    break;
                default:
                    // Broadcast serialization with a cached out, only need to change the part and ready to push to datastream
                    StreamRecord<GraphOp> record = getInstance().asRecord();
                    int skipTsAndGraphOpFlag = 2 + (record.hasTimestamp() ? 8 : 0); // Add timestamp vs no-timestamp of the StreamElement serialization
                    out.skipBytesToWrite(skipTsAndGraphOpFlag);
                    out.writeShort(record.getValue().partId); // Change the part id to the new one
                    out.skipBytesToWrite(broadcastStarted - skipTsAndGraphOpFlag - 6); // Make the position equal
            }
        }

        @Override
        public void read(DataInputView in) throws IOException {
            mainSerializationDelegate.read(in);
        }

        void broadcastStart() {
            broadcastStarted = 1;
        }

        void broadcastFinish() {
            broadcastStarted = 0;
        }

    }

    static OutputWithChainingCheck<StreamRecord<GraphOp>>[] extractOutputs(Output<StreamRecord<GraphOp>> output){
        try {
            List<OutputWithChainingCheck<StreamRecord<GraphOp>>> tmpOutputs = new ArrayList<>(2 << 4);
            Queue<Output<StreamRecord<GraphOp>>> queue = new ArrayDeque<>(List.of(output));
            Output<StreamRecord<GraphOp>> tmp;
            while ((tmp = queue.poll()) != null) {
                if (tmp instanceof CountingOutput) {
                    queue.add((Output<StreamRecord<GraphOp>>) countingOutputOutputField.get(tmp));
                }else if(tmp instanceof BroadcastingOutputCollector){
                    queue.addAll(List.of(((BroadcastingOutputCollector<GraphOp>) tmp).outputs));
                }else if(tmp instanceof OutputWithChainingCheck){
                    tmpOutputs.add((OutputWithChainingCheck<StreamRecord<GraphOp>>) tmp);
                }else{
                    throw new IllegalStateException("Unhandled output type" + tmp.getClass());
                }
            }
            return tmpOutputs.toArray(OutputWithChainingCheck[]::new);
        }catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
