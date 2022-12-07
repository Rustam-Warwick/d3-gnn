package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * TAIL Operator for handling Stream Iterations
 * @implNote Input to this Operator should be already partitioned as it is expected for the iteration BODY
 */
public class IterationTailOperator<IN> extends AbstractStreamOperator<Void> implements OneInputStreamOperator<IN, Void> {

    /**
     * ID Of the Head iteration to identify the buffer
     */
    protected final int iterationID;

    /**
     * Full ID of {@link IterationChannel}
     */
    protected final IterationChannelKey channelID;

    protected IterationChannel.IterationQueue<StreamRecord<IN>> iterationQueue;

    public IterationTailOperator(int iterationID, StreamOperatorParameters<Void> parameters) {
        this.iterationID = iterationID;
        this.channelID = new IterationChannelKey(parameters.getContainingTask().getEnvironment().getJobID(), iterationID, parameters.getContainingTask().getEnvironment().getTaskInfo().getAttemptNumber(), parameters.getContainingTask().getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        this.processingTimeService = parameters.getProcessingTimeService();
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        IterationChannel<StreamRecord<IN>> channel = IterationChannelBroker.getBroker().getIterationChannel(channelID);
        iterationQueue = channel.addProducer(getOperatorID());
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        iterationQueue.add(element);
    }
}
