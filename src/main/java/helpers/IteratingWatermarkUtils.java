package helpers;

import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Utility class that converts watermarks to iterative watermarks for
 * across replica synchronization
 */
public class IteratingWatermarkUtils {
    private static long CHECK_MASK = Long.rotateRight(3, 2);
    /**
     * Is this watermark an iterative one, or is it valid watermark
     * @param watermark Watermark
     * @return Is iterative or not
     */
    public static long getIterativeWatermark(Watermark watermark){
        long timestamp = watermark.getTimestamp();
        return (timestamp & CHECK_MASK);
    }

    public static Watermark generateNewWatermarkIteration(Watermark watermark, long state){
        long timestamp  = watermark.getTimestamp();
        long watermarkNew = timestamp | Long.rotateRight(state, 2);
        return new Watermark(watermarkNew);
    }

}
