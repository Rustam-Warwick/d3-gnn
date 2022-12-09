package functions.metrics;

import org.apache.flink.metrics.Counter;

/**
 * Returns the average of the counted values, on getCount()
 * Use joinly with MetricView to track latency
 */
public class MovingAverageCounter implements Counter {
    final int sizeWindow;
    long[] values;
    int count;

    public MovingAverageCounter(int sizeWindow) {
        this.sizeWindow = sizeWindow;
        values = new long[sizeWindow];
    }

    @Override
    public void inc() {
        values[count % sizeWindow] = 1;
        count++;
    }

    @Override
    public void inc(long n) {
        values[count % sizeWindow] = n;
        count++;
    }

    @Override
    public void dec() {
        throw new IllegalStateException("Moving average cannot be decremented");
    }

    @Override
    public void dec(long n) {
        throw new IllegalStateException("Moving average cannot be decremented");
    }

    @Override
    public long getCount() {
        double avg = 0;
        int t = 1;
        for (int i = Math.max(0, count - sizeWindow); i < count; i++) {
            avg += (values[i % sizeWindow] - avg) / t;
            ++t;
        }
        return (long) avg;
    }
}
