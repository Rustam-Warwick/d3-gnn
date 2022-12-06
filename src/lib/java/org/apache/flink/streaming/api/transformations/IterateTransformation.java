package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a Transformation for an Iteration
 * HEAD -> Single transformation that directs all messages to the iteration body
 * BODY -> Transformation that processes HEAD messages jointly with its external messages
 * FEEDBACK -> Transformations that send messages to iteration tails
 * TAIL -> Transformation that directs messages sent to it to the iteration HEAD
 * @param <T> The type of elements in the iteration going to the iteration BODY
 * @implNote Do not use this transformation directly
 */
public class IterateTransformation<T> extends Transformation<T> {

    /**
     * The body of this iteration to which all messages are sent to
     * Note that output of this transformation can be different from T
     */
    protected PhysicalTransformation<?> iterationBodyTransformation;

    /**
     * All the transformations that are going to be fed back to the iteration head
     */
    protected Set<Transformation<T>> iterationFeedbackTransformations = new HashSet<>(3);

    public IterateTransformation(PhysicalTransformation<?> iterationBodyTransformation) {
        super(String.format("%s[HEAD]", iterationBodyTransformation.getName()), (TypeInformation<T>) iterationBodyTransformation.getInputs().get(0).getOutputType(), iterationBodyTransformation.getParallelism());
        this.iterationBodyTransformation = iterationBodyTransformation;
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        return iterationBodyTransformation.getTransitivePredecessors();
    }

    /**
     * {@inheritDoc}
     * Delegate to StartTransformation
     */
    @Override
    public List<Transformation<?>> getInputs() {
        return iterationBodyTransformation.getInputs();
    }

    /**
     * Get the transformation where this iteration starts
     */
    public PhysicalTransformation<?> getIterationBodyTransformation() {
        return iterationBodyTransformation;
    }

    /**
     * Get the transformations where this iteration ends
     */
    public Set<Transformation<T>> getIterationFeedbackTransformations() {
        return iterationFeedbackTransformations;
    }

    /**
     * Add the Given Transformation as one of the iterations in the pipeline
     */
    public void addFeedbackEdge(Transformation<T> feedbackTransformation){
        Preconditions.checkState(feedbackTransformation.getOutputType().equals(getOutputType()), String.format("Output Type %s is %s, expected %s", feedbackTransformation, feedbackTransformation.getOutputType(), getOutputType()));
        iterationFeedbackTransformations.add(feedbackTransformation);
    }

}
