package org.apache.flink.streaming.runtime.tasks;

import elements.GraphOp;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.List;

/**
 * Special Counting Output Collector for {@link GraphOp}.
 * <ul>
 *     <li> Counting Output Metrics </li>
 *     <li> Handling broadcast & selective broadcast messages </li>
 * </ul>
 * @implNote Assumes the underlying serializer is {@link typeinfo.graphopinfo.GraphOpSerializer} joined with {@link org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer}
 * @implNote Broadcast and selective broadcast messages only work for {@link RecordWriterOutput} channels
 * @implNote Broadcast messages are only counted as a single record output @todo Fix later
 */
public class CountingBroadcastingGraphOutputCollector extends BroadcastingOutputCollector<GraphOp> {

    static Field outputTagField;

    static Field recordWriterField;

    static Field serializationDelegateField;

    static {
        try{
            outputTagField = RecordWriterOutput.class.getDeclaredField("outputTag");
            recordWriterField = RecordWriterOutput.class.getDeclaredField("recordWriter");
            serializationDelegateField = RecordWriterOutput.class.getDeclaredField("serializationDelegate");
            outputTagField.setAccessible(true);
            recordWriterField.setAccessible(true);
            serializationDelegateField.setAccessible(true);
        }catch (Exception e){
            throw new RuntimeException("Could not initialization GraphOutputCollected, turn off Security manager");
        }
    }

    /**
     * Operator metrics counter
     */
    protected final Counter numRecordsOutCounter;

    /**
     * Reference to the fields in the {@link RecordWriterOutput}
     */
    protected final Tuple3<OutputTag<GraphOp>, RecordWriter<SerializationDelegate<StreamElement>>, HelperSerializationDelegate>[] outputInternalInfo;

    public CountingBroadcastingGraphOutputCollector(Output<StreamRecord<GraphOp>> output, Counter numRecordsOutCounter) {
        super(output instanceof BroadcastingOutputCollector? ((BroadcastingOutputCollector) output).outputs:new Output[]{output});
        this.numRecordsOutCounter = numRecordsOutCounter;
        outputInternalInfo = new Tuple3[outputs.length];
        try {
            for (int i = 0; i < outputs.length; i++) {
                if (outputs[i] instanceof RecordWriterOutput) {
                    OutputTag<GraphOp> tag = (OutputTag<GraphOp>) outputTagField.get(outputs[i]);
                    RecordWriter<SerializationDelegate<StreamElement>> recordWriter = (RecordWriter<SerializationDelegate<StreamElement>>) recordWriterField.get(outputs[i]);
                    SerializationDelegate<StreamElement> serializationDelegateOld = (SerializationDelegate<StreamElement>) serializationDelegateField.get(outputs[i]);
                    HelperSerializationDelegate serializationDelegate = new HelperSerializationDelegate(serializationDelegateOld);
                    serializationDelegateField.set(outputs[i], serializationDelegate);
                    outputInternalInfo[i] = Tuple3.of(tag, recordWriter, serializationDelegate);
                }
            }
        }catch (Exception e){
            throw new RuntimeException("Could not initialize GraphOutputCollected, turn off Security Manager");
        }
    }

    @Override
    public void collect(StreamRecord<GraphOp> record) {
        numRecordsOutCounter.inc();
        for (Output<StreamRecord<GraphOp>> output : outputs) {
            output.collect(record);
        }
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        numRecordsOutCounter.inc();
        for (Output<StreamRecord<GraphOp>> output : outputs) {
            output.collect(outputTag, record);
        }
    }

    /**
     * Broadcast single messages to all output channels in the forward layer
     */
    public void broadcast(StreamRecord<GraphOp> record){
        for (Tuple3<OutputTag<GraphOp>, RecordWriter<SerializationDelegate<StreamElement>>, HelperSerializationDelegate> info : outputInternalInfo) {
            if(info == null) continue;
            if(info.f0 == null){
                numRecordsOutCounter.inc();
                info.f2.setInstance(record);
                try {
                    info.f1.broadcastEmit(info.f2);
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                }
                return;
            }
        }
        throw new IllegalStateException("No output responsible for broadcast message");
    }

    /**
     * Broadcast single message to all output channels for an {@link OutputTag} output
     */
    public void broadcast(OutputTag<GraphOp> outputTag, StreamRecord<GraphOp> record){
        for (Tuple3<OutputTag<GraphOp>, RecordWriter<SerializationDelegate<StreamElement>>, HelperSerializationDelegate> info : outputInternalInfo) {
            if(info == null) continue;
            if(OutputTag.isResponsibleFor(outputTag, info.f0)){
                numRecordsOutCounter.inc();
                info.f2.setInstance(record);
                try {
                    info.f1.broadcastEmit(info.f2);
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                }
                return;
            }
        }
        throw new IllegalStateException("No output responsible for broadcast message");
    }

    /**
     * Do a selective broadcast of a single {@link GraphOp} to multiple parts in the forward by only changing the part id
     */
    public void broadcast(StreamRecord<GraphOp> record, List<Short> selectedParts){
        for (int i = 0; i < outputInternalInfo.length; i++) {
            if(outputInternalInfo[i] == null) continue;
            if(outputInternalInfo[i].f0 == null){
                outputInternalInfo[i].f2.broadcastStart();
                for (short selectedPart : selectedParts) {
                    record.getValue().setPartId(selectedPart);
                    outputs[i].collect(record);
                }
                numRecordsOutCounter.inc(selectedParts.size());
                outputInternalInfo[i].f2.broadcastFinish();
                return;
            }
        }
        throw new IllegalStateException("No output responsible for broadcast message");
    }

    public void broadcast(OutputTag<GraphOp> outputTag, StreamRecord<GraphOp> record, List<Short> selectedParts){
        for (int i = 0; i < outputInternalInfo.length; i++) {
            if(outputInternalInfo[i] == null) continue;
            if(OutputTag.isResponsibleFor(outputTag, outputInternalInfo[i].f0)){
                outputInternalInfo[i].f2.broadcastStart();
                for (short selectedPart : selectedParts) {
                    record.getValue().setPartId(selectedPart);
                    outputs[i].collect(outputTag, record);
                }
                numRecordsOutCounter.inc(selectedParts.size());
                outputInternalInfo[i].f2.broadcastFinish();
                return;
            }
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
         *  0 -> Normal mode of execution, need to delegate everything to mainSerializationDelegate
         *  1 -> Started broadcasting but haven't yet received the first element of broadcast
         *  >1 -> Broadcasting and the out already holds the serialized value and this int is the capacity of the serialized value
         * </p>
         *
         */
        int broadcastStarted = 0;

        public HelperSerializationDelegate(SerializationDelegate<StreamElement> mainSerializationDelegate) {
            super(null);
            this.mainSerializationDelegate = mainSerializationDelegate;
        }

        @Override
        public void setInstance(StreamElement instance) {
            mainSerializationDelegate.setInstance(instance);
        }

        @Override
        public StreamElement getInstance() {
            return mainSerializationDelegate.getInstance();
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            switch (broadcastStarted){
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
                    int skipTsAndGraphOpFlag = 2 + (record.hasTimestamp()?8:0); // Add timestamp vs no-timestamp of the StreamElement serialization
                    out.skipBytesToWrite(skipTsAndGraphOpFlag);
                    out.writeShort(record.getValue().partId); // Change the part id to the new one
                    out.skipBytesToWrite(broadcastStarted - skipTsAndGraphOpFlag - 6); // Make the position equal
            }
        }

        @Override
        public void read(DataInputView in) throws IOException {
            mainSerializationDelegate.read(in);
        }

        void broadcastStart(){
            broadcastStarted = 1;
        }

        void broadcastFinish(){
            broadcastStarted = 0;
        }

    }

}
