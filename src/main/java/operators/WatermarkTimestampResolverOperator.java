package operators;

import elements.GraphOp;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * FiltersOutWatermarks that are still iterating in the previous operator
 * Utility class that converts watermarks to iterative watermarks for
 * across replica synchronization.
 *
 * @implNote Only takes first 2 bits of timestamp for enconding iteration number. So no elements.iterations of size > 4
 * @implNote Iteration numbers go up, so 0..3
 * @implNote Using bitwise operators, in case the system does not use 2's complement
 */
public class WatermarkTimestampResolverOperator extends AbstractStreamOperator<GraphOp> implements OneInputStreamOperator<GraphOp, GraphOp> {

    public WatermarkTimestampResolverOperator() {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    /**
     * Get the iteration number of this timestamp
     *
     * @param timestamp Timestamp of the watermark
     * @return Is iterative or not
     */
    public static long getIterationNumber(long timestamp) {
        return timestamp & 3;
    }

    public static long setIterationNumber(long timestamp, long iterationNumber) {
        return (timestamp >> 2 << 2) | iterationNumber;
    }

    /**
     * Encodes normal watermark into iteration timestamp
     *
     * @param timestamp normal timestamp
     * @return iteration timestamp
     */
    public static long encode(long timestamp) {
        return timestamp >> 2 << 2;
    }

    /**
     * Decode the iteration timestamp into a normal one
     *
     * @param timestamp iteration timestamp
     * @return normal timestamp
     */
    public static long decode(long timestamp) {
        return timestamp;
    }

    @Override
    public void processElement(StreamRecord<GraphOp> element) throws Exception {
        element.setTimestamp(element.getValue().getTimestamp()); // Make timestamp the timestamp of the graphop
        output.collect(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (mark.getTimestamp() % 4 == 3) {
            super.processWatermark(mark);
        }
    }
}
