package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.JobID;

import java.util.Objects;

/**
 * Key that is used to generate iteration channel IDs
 */
public class IterationChannelKey {

    private final JobID jobID;

    private final int iterationID;

    private final int attemptNumber;

    private final int subtaskIndex;

    public IterationChannelKey(JobID jobID, int iterationID, int attemptNumber, int subtaskIndex) {
        this.jobID = jobID;
        this.iterationID = iterationID;
        this.attemptNumber = attemptNumber;
        this.subtaskIndex = subtaskIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IterationChannelKey that = (IterationChannelKey) o;
        return iterationID == that.iterationID && attemptNumber == that.attemptNumber && subtaskIndex == that.subtaskIndex && Objects.equals(jobID, that.jobID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobID, iterationID, attemptNumber, subtaskIndex);
    }
}
