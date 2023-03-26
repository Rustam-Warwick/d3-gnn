package org.apache.flink.streaming.api.operators;

import ai.djl.ndarray.BaseNDManager;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Operator that buffers the values until the operator is closed when it finally emits everything in bulk
 *
 * @param <IN> Type of element both to input and output
 */
public class FullBufferOperator<IN> extends AbstractStreamOperator<IN> implements OneInputStreamOperator<IN, IN> {
    private final Queue<StreamRecord<IN>> buffer = new ArrayDeque<>();

    @Override
    public void open() throws Exception {
        super.open();
        BaseNDManager.getManager().delay(); // Delay
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        buffer.add(element);
    }

    public void sendElements() throws Exception {
        StreamRecord<IN> tmpEl;
        while ((tmpEl = buffer.poll()) != null) {
            output.collect(tmpEl);
        }
        BaseNDManager.getManager().resume();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (mark.getTimestamp() == Long.MAX_VALUE) sendElements();
        super.processWatermark(mark);
    }
}
