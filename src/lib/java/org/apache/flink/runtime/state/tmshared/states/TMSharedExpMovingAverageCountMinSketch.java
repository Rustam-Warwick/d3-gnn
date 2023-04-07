package org.apache.flink.runtime.state.tmshared.states;

import com.clearspring.analytics.hash.MurmurHash;
import org.apache.flink.runtime.state.tmshared.TMSharedState;
import org.apache.flink.util.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link TMSharedState} that calculate the exp mean of string counts backed by 3 CountMin data structures
 */
public class TMSharedExpMovingAverageCountMinSketch extends TMSharedState{

    protected final int depth;

    protected final int width;

    protected final long[][][] tables;

    protected long size;

    final protected double eps;

    final protected long movingAverageIntervalMs;

    final protected double momentum;

    final protected double invMomentum;

    final protected double confidence;

    final protected AtomicInteger currentlyActive = new AtomicInteger(0);

    final protected TimerTask timerTask = new ExpMeanUpdateTask();

    final protected Timer timer = new Timer();

    public TMSharedExpMovingAverageCountMinSketch(int depth, int width, long movingAverageIntervalMs, double momentum) {
        Preconditions.checkState(momentum >= 0 && momentum <= 1, "momentum should be between [0-1]");
        Preconditions.checkState(movingAverageIntervalMs > 0, "Update interval should be > 0ms");
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        this.tables = new long[3][depth][width];
        this.movingAverageIntervalMs = movingAverageIntervalMs;
        this.momentum = momentum;
        this.invMomentum = 1 - momentum;
        timer.schedule(timerTask, movingAverageIntervalMs, movingAverageIntervalMs);
    }

    public TMSharedExpMovingAverageCountMinSketch(double epsOfTotalCount, double confidence, long movingAverageIntervalMs, double momentum) {
        Preconditions.checkState(momentum >= 0 && momentum <= 1, "momentum should be between [0-1]");
        Preconditions.checkState(movingAverageIntervalMs > 0, "Update interval should be > 0ms");
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(2 / epsOfTotalCount);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        this.tables = new long[3][depth][width];
        this.movingAverageIntervalMs = movingAverageIntervalMs;
        this.momentum = momentum;
        this.invMomentum = 1 - momentum;
        timer.schedule(timerTask, movingAverageIntervalMs, movingAverageIntervalMs);
    }

    public double getRelativeError() {
        return eps;
    }

    public double getConfidence() {
        return confidence;
    }

    public void add(String item, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        int activeTable = currentlyActive.get();
        int[] buckets = getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            tables[activeTable][i][buckets[i]] += count;
        }
        checkSizeAfterAdd(item, count);
    }

    public long estimateCount(String item) {
        long res = Long.MAX_VALUE;
        int[] buckets = getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, tables[0][i][buckets[i]]);
        }
        return res;
    }

    public int[] getHashBuckets(String key, int hashCount, int max) {
        byte[] b;
        b = key.getBytes(StandardCharsets.UTF_8);
        return getHashBuckets(b, hashCount, max);
    }

    private int[] getHashBuckets(byte[] b, int hashCount, int max) {
        int[] result = new int[hashCount];
        int hash1 = MurmurHash.hash(b, b.length, 0);
        int hash2 = MurmurHash.hash(b, b.length, hash1);
        for (int i = 0; i < hashCount; i++) {
            result[i] = Math.abs((hash1 + i * hash2) % max);
        }
        return result;
    }

    private void checkSizeAfterAdd(String item, long count) {
        long previousSize = size;
        size += count;
        checkSizeAfterOperation(previousSize,  size);
    }

    private static void checkSizeAfterOperation(long previousSize, long newSize) {
        if (newSize < previousSize) {
            throw new IllegalStateException("Overflow error: the size after calling add`" +
                    "` is smaller than the previous size. " +
                    "Previous size: " + previousSize +
                    ", New size: " + newSize);
        }
    }

    private class ExpMeanUpdateTask extends TimerTask{
        @Override
        public void run() {
            int previouslyActive = currentlyActive.get();
            if(previouslyActive == 0){
                currentlyActive.set(1);
                return;
            }
            currentlyActive.set((previouslyActive % 2) + 1);
            for (int i = 0; i < depth; i++) {
                for (int j = 0; j < width; j++) {
                    tables[0][i][j] = (long) (tables[0][i][j] * momentum + tables[previouslyActive][i][j] * invMomentum);
                }
                Arrays.fill(tables[previouslyActive][i], 0);
            }
        }
    }

    @Override
    public void clear() {
        timer.cancel();
    }
}
