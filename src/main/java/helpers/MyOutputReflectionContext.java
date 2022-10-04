package helpers;


import org.apache.flink.iteration.broadcast.ChainingBroadcastOutput;
import org.apache.flink.iteration.broadcast.OutputReflectionContext;
import org.apache.flink.iteration.utils.ReflectionUtils;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

/**
 * Helper class with extension of FLink-ML iteration. Used to create individual outputs in the @link{BaseWrapperOperator}
 */
public class MyOutputReflectionContext extends OutputReflectionContext {
    private final Field recordWriterTagField;
    private final Field recordWriterField;
    private final Field recordWriterNumChannelsField;

    public MyOutputReflectionContext() {
        try {

            this.recordWriterTagField =
                    ReflectionUtils.getClassField(RecordWriterOutput.class, "outputTag");

            this.recordWriterField = ReflectionUtils.getClassField(RecordWriterOutput.class, "recordWriter");
            this.recordWriterNumChannelsField = ReflectionUtils.getClassField(RecordWriter.class, "numberOfChannels");

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the OutputReflectionContext", e);
        }
    }

    public int getNumChannels(Object output) {
        return ReflectionUtils.<Integer>getFieldValue(ReflectionUtils.<RecordWriter>getFieldValue(output, recordWriterField), recordWriterNumChannelsField);
    }

    public <OUT> ChainingBroadcastOutput<OUT> createChainingBroadcastOutput(Output<StreamRecord<Object>> rawOutput, OutputTag outputTag) {
        try {
            Constructor<ChainingBroadcastOutput> chainingBroadcastOutputConstructor = ChainingBroadcastOutput.class.getDeclaredConstructor(Output.class, OutputTag.class);
            chainingBroadcastOutputConstructor.setAccessible(true);
            ChainingBroadcastOutput<OUT> broadcastOutput = chainingBroadcastOutputConstructor.newInstance(rawOutput, outputTag);
            chainingBroadcastOutputConstructor.setAccessible(false);
            return broadcastOutput;
        } catch (Exception e) {
            return null;
        }

    }

    public OutputTag<?> getRecordWriterOutputTag(Object output) {
        return ReflectionUtils.getFieldValue(output, recordWriterTagField);
    }
}
