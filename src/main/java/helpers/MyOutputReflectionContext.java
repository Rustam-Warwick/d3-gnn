package helpers;


import org.apache.flink.iteration.broadcast.OutputReflectionContext;
import org.apache.flink.iteration.utils.ReflectionUtils;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Field;

public class MyOutputReflectionContext extends OutputReflectionContext {
    private final Field recordWriterTagField;


    public MyOutputReflectionContext() {
        try {

            this.recordWriterTagField =
                    ReflectionUtils.getClassField(RecordWriterOutput.class, "outputTag");

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the OutputReflectionContext", e);
        }
    }

    public OutputTag<?> getRecordWriterOutputTag(Object output) {
        return ReflectionUtils.getFieldValue(output, recordWriterTagField);
    }
}
