package org.apache.flink.streaming.runtime.tasks;

import elements.GraphOp;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
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

/**
 * Special Counting Output Collector for Graph.
 * <ul>
 *     <li> Counting Output Metric </li>
 *     <li> Handling broadcast messages </li>
 * </ul>
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

    protected final Counter numRecordsOutCounter;

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

    public void broadcast(StreamRecord<GraphOp> record){
        for (Tuple3<OutputTag<GraphOp>, RecordWriter<SerializationDelegate<StreamElement>>, HelperSerializationDelegate> info : outputInternalInfo) {
            if(info == null) continue;
            if(info.f0 == null){
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

    public void broadcast(OutputTag<GraphOp> outputTag, StreamRecord<GraphOp> record){
        for (Tuple3<OutputTag<GraphOp>, RecordWriter<SerializationDelegate<StreamElement>>, HelperSerializationDelegate> info : outputInternalInfo) {
            if(info == null) continue;
            if(OutputTag.isResponsibleFor(outputTag, info.f0)){
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

    public void broadcast(StreamRecord<GraphOp> record, short ...selectedParts){
        throw new IllegalStateException("No output responsible for broadcast message");
    }

    public void broadcast(OutputTag<GraphOp> outputTag, StreamRecord<GraphOp> record, short ...selectedParts){
        throw new IllegalStateException("No output responsible for broadcast message");
    }

    /**
     * Wrapper around {@link RecordWriterOutput} {@link SerializationDelegate}
     * Helps to handle single serialization for multiple part outputs
     */
    static class HelperSerializationDelegate extends SerializationDelegate<StreamElement>{

        /**
         * Main {@link SerializationDelegate}
         */
        final SerializationDelegate<StreamElement> mainSerializationDelegate;

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
            mainSerializationDelegate.write(out);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            mainSerializationDelegate.read(in);
        }
    }

}
