package helpers;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import java.util.Arrays;

public class MyHistorgram implements Histogram {
    public final long[] results;
    public int filled;

    public MyHistorgram() {
        results = new long[100];
        filled = 0;
        Arrays.fill(results, 0);
    }

    @Override
    public void update(long value) {
        if (filled == 100) {
            for (int i = 1; i < 100; i++) {
                results[i - 1] = results[i];
            }
            results[99] = value;
        } else {
            results[filled++] = value;
        }
    }

    @Override
    public long getCount() {
        return filled;
    }

    @Override
    public HistogramStatistics getStatistics() {
        return new HistogramStatistics() {
            @Override
            public double getQuantile(double quantile) {
                return 0;
            }

            @Override
            public long[] getValues() {
                return results;
            }

            @Override
            public int size() {
                return filled;
            }

            @Override
            public double getMean() {
                long sum = Arrays.stream(results).sum();
                return (double) sum / filled;
            }

            @Override
            public double getStdDev() {
                double mean = getMean();
                double sumVar = 0;
                for (int i = 0; i < filled; i++) {
                    sumVar += Math.pow(results[i] - mean, 2);
                }

                return Math.sqrt(sumVar / filled);
            }

            @Override
            public long getMax() {
                return Arrays.stream(results).max().getAsLong();
            }

            @Override
            public long getMin() {
                return Arrays.stream(results).min().getAsLong();
            }
        };
    }
}

