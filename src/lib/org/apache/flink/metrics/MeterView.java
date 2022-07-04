
package org.apache.flink.metrics;

import org.apache.flink.annotation.Internal;

/**
 * A MeterView provides an average rate of events per second over a given time period.
 *
 * <p>The primary advantage of this class is that the rate is neither updated by the computing
 * thread nor for every event. Instead, a history of counts is maintained that is updated in regular
 * intervals by a background thread. From this history a rate is derived on demand, which represents
 * the average rate of events over the given time span.
 *
 * <p>Setting the time span to a low value reduces memory-consumption and will more accurately
 * report short-term changes. The minimum value possible is {@link View#UPDATE_INTERVAL_SECONDS}. A
 * high value in turn increases memory-consumption, since a longer history has to be maintained, but
 * will result in smoother transitions between rates.
 *
 * <p>The events are counted by a {@link Counter}.
 */
@Internal
public class MeterView implements Meter, View {

    private static final int DEFAULT_TIME_SPAN_IN_SECONDS = View.UPDATE_INTERVAL_SECONDS;

    /** The underlying counter maintaining the count. */
    private final Counter counter;
    /** The time-span over which the average is calculated. */
    private final int timeSpanInSeconds;
    /** Circular array containing the history of values. */
    private final long[] values;
    /** The index in the array for the current time. */
    private int time = 0;
    /** The last rate we computed. */
    private double currentRate = 0;

    public MeterView(int timeSpanInSeconds) {
        this(new SimpleCounter(), timeSpanInSeconds);
    }

    public MeterView(Counter counter) {
        this(counter, DEFAULT_TIME_SPAN_IN_SECONDS);
    }

    public MeterView(Counter counter, int timeSpanInSeconds) {
        this.counter = counter;
        // the time-span must be larger than the update-interval as otherwise the array has a size
        // of 1,
        // for which no rate can be computed as no distinct before/after measurement exists.
        this.timeSpanInSeconds =
                Math.max(
                        timeSpanInSeconds - (timeSpanInSeconds % UPDATE_INTERVAL_SECONDS),
                        UPDATE_INTERVAL_SECONDS);
        this.values = new long[this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];
    }

    @Override
    public void markEvent() {
        this.counter.inc();
    }

    @Override
    public void markEvent(long n) {
        this.counter.inc(n);
    }

    @Override
    public long getCount() {
        return counter.getCount();
    }

    @Override
    public double getRate() {
        return currentRate;
    }

    @Override
    public void update() {
        time = (time + 1) % values.length;
        values[time] = counter.getCount();
        currentRate =
                ((double) (values[time] - values[(time + 1) % values.length]) / timeSpanInSeconds);
    }
}
