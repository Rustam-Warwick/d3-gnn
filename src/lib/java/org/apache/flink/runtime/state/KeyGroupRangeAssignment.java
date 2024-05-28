package org.apache.flink.runtime.state;

import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

public final class KeyGroupRangeAssignment {
    public static final int DEFAULT_LOWER_BOUND_MAX_PARALLELISM = 128;
    public static final int UPPER_BOUND_MAX_PARALLELISM = 32768;

    private KeyGroupRangeAssignment() {
        throw new AssertionError();
    }

    public static int assignKeyToParallelOperator(Object key, int maxParallelism, int parallelism) {
        Preconditions.checkNotNull(key, "Assigned key must not be null!");
        return computeOperatorIndexForKeyGroup(maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism));
    }

    public static int assignToKeyGroup(Object key, int maxParallelism) {
        Preconditions.checkNotNull(key, "Assigned key must not be null!");
        if (key instanceof PartNumber) {
            return key.hashCode() % maxParallelism;
        }
        return computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
    }

    public static int computeKeyGroupForKeyHash(int keyHash, int maxParallelism) {
        return MathUtils.murmurHash(keyHash) % maxParallelism;
    }

    public static KeyGroupRange computeKeyGroupRangeForOperatorIndex(int maxParallelism, int parallelism, int operatorIndex) {
        checkParallelismPreconditions(parallelism);
        checkParallelismPreconditions(maxParallelism);
        Preconditions.checkArgument(maxParallelism >= parallelism, "Maximum parallelism must not be smaller than parallelism.");
        int start = (operatorIndex * maxParallelism + parallelism - 1) / parallelism;
        int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
        return new KeyGroupRange(start, end);
    }

    public static int computeOperatorIndexForKeyGroup(int maxParallelism, int parallelism, int keyGroupId) {
        return keyGroupId * parallelism / maxParallelism;
    }

    public static int computeDefaultMaxParallelism(int operatorParallelism) {
        checkParallelismPreconditions(operatorParallelism);
        return Math.min(Math.max(MathUtils.roundUpToPowerOfTwo(operatorParallelism + operatorParallelism / 2), 128), 32768);
    }

    public static void checkParallelismPreconditions(int parallelism) {
        Preconditions.checkArgument(parallelism > 0 && parallelism <= 32768, "Operator parallelism not within bounds: " + parallelism);
    }
}


