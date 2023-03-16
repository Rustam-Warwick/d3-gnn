package org.apache.flink.streaming.api.operators.iteration;

import ai.djl.ndarray.LifeCycleControl;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.ThrowingConsumer;

/**
 * TAIL Operator for handling Stream Iterations
 *
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

    /**
     * If the elements in this channel are instances of {@link LifeCycleControl}. If it is the case, need to delay on adding to buffer
     */
    protected final boolean hasLifeCycleControl;

    /**
     * Consumer for the incoming elements
     */
    protected final ThrowingConsumer<StreamRecord<IN>, Exception> handler;
    /**
     * {@link org.apache.flink.streaming.api.operators.iteration.IterationChannel.IterationQueue} for sending iterative messages
     */
    protected IterationChannel.IterationQueue<StreamRecord<IN>> iterationQueue;


    public IterationTailOperator(int iterationID, StreamOperatorParameters<Void> parameters, boolean hasLifeCycleControl) {
        this.iterationID = iterationID;
        this.hasLifeCycleControl = hasLifeCycleControl;
        this.channelID = new IterationChannelKey(parameters.getContainingTask().getEnvironment().getJobID(), iterationID, parameters.getContainingTask().getEnvironment().getTaskInfo().getAttemptNumber(), parameters.getContainingTask().getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        this.processingTimeService = parameters.getProcessingTimeService();
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
        if (parameters.getStreamConfig().getChainIndex() == 1) handler = this::processWithReuse;
        else handler = this::processWithoutReuse;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        IterationChannel<StreamRecord<IN>> channel = IterationChannelBroker.getBroker().getIterationChannel(channelID);
        iterationQueue = channel.addProducer(getOperatorID());
    }

    /**
     * Reuse the {@link StreamRecord}
     */
    public void processWithReuse(StreamRecord<IN> element) throws Exception {
        iterationQueue.add(element);
    }

    /**
     * Create a Shallow copy of {@link StreamRecord} before processing
     */
    public void processWithoutReuse(StreamRecord<IN> element) throws Exception {
        iterationQueue.add(element.copy(element.getValue()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (hasLifeCycleControl) ((LifeCycleControl) element.getValue()).delay(); // Should be resumed on HEAD
        handler.accept(element);
    }
}
