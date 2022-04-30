package helpers;

import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Utility class that converts watermarks to iterative watermarks for
 * across replica synchronization.
 * @implNote Only takes first 2 bits of timestamp for enconding iteration number. So no iterations of size > 4
 * @implNote Iteration numbers go up, so 0..3
 * @implNote Using bitwise operators, in case the system does not use 2's complement
 */
public class IteratingWatermarkUtils {

    public static boolean isIterationTimestamp(long timestamp){
        return (Long.rotateRight(1, 1) & timestamp) != 0;
    }


    /**
     * Get the iteration number of this timestamp
     * @param timestamp Timestamp of the watermark
     * @return Is iterative or not
     */
    public static long getIterationNumber(long timestamp){
        assert isIterationTimestamp(timestamp);
        return timestamp & 3;
    }

    public static long addIterationNumber(long timestamp, long iterationNumber){
        assert isIterationTimestamp(timestamp);
        return (timestamp >> 2 << 2) | iterationNumber;
    }

    /**
     * Encodes normal watermark into iteration timestamp
     * @param timestamp normal timestamp
     * @return iteration timestamp
     */
    public static long encode(long timestamp){
         return (Long.rotateRight(1,1) | timestamp) >> 2 << 2;
    }

    /**
     * Decode the iteration timestamp into a normal one
     * @param timestamp iteration timestamp
     * @return normal timestamp
     */
    public static long decode(long timestamp){
        return timestamp << 1 >> 1; // Get rid of the sign
    }



    /**
     * Decrement the watermark
     * @param timestamp
     * @return
     */
    public static Watermark getDecrementedWatermark(long timestamp){
        long iterationNumber = getIterationNumber(timestamp);
        assert iterationNumber > 0;
        long noIterationTimestamp = timestamp ^ Long.rotateRight(iterationNumber, 2); // just the timestamp, no iteration data
        long newIterationTimestamp = noIterationTimestamp | Long.rotateRight(iterationNumber - 1, 2);
        return new Watermark(newIterationTimestamp);
    }

}
