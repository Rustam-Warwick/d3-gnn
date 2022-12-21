package org.apache.flink.streaming.api.operators;

/**
 * Operators that expose the {@link InternalTimerService} should implement this interface
 * <p>
 * If you want {@link org.apache.flink.streaming.api.operators.iteration.WrapperIterationHeadOperator}
 * to wait until timers are finished before termination, operator should be exposing its timer service
 * </p>
 */
public interface ExposingInternalTimerService {

    /**
     * Return internal timer service
     */
    InternalTimerService<?> getInternalTimerService();
}
