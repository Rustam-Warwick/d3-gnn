package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a Transformation for an Iteration
 * @param <T> Output type that goes into the iteration
 */
public class IterateTransformation<T> extends Transformation<T> {

    /**
     * The original transformation to which this should be jointly sent
     * Note that output of iterationStartTransformation can be different from T
     */
    protected PhysicalTransformation<?> iterationStartTransformation;

    /**
     * All the transformations that are the tails for this transformation
     */
    protected Set<Transformation<T>> iterationTailTransformations = new HashSet<>(3);

    public IterateTransformation(PhysicalTransformation<?> iterationStartTransformation) {
        super(String.format("%s[HEAD]", iterationStartTransformation.getName()), (TypeInformation<T>) (iterationStartTransformation.getInputs().isEmpty()? iterationStartTransformation.getOutputType(): iterationStartTransformation.getInputs().get(0).getOutputType()), iterationStartTransformation.getParallelism());
        this.iterationStartTransformation = iterationStartTransformation;
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        return iterationStartTransformation.getTransitivePredecessors();
    }

    /**
     * {@inheritDoc}
     * Delegate to StartTransformation
     */
    @Override
    public List<Transformation<?>> getInputs() {
        return iterationStartTransformation.getInputs();
    }

    /**
     * Get the trasnformation where this iteration starts
     */
    public PhysicalTransformation<?> getIterationStartTransformation() {
        return iterationStartTransformation;
    }

    /**
     * Get the transformations where this iteration ends
     */
    public Set<Transformation<T>> getIterationTailTransformations() {
        return iterationTailTransformations;
    }

    /**
     * Add the Given Transformation as one of the iterations in the pipeline
     */
    public void addFeedbackEdge(Transformation<T> feedbackTransformation){
        Preconditions.checkState(feedbackTransformation.getOutputType().equals(getOutputType()));
        iterationTailTransformations.add(feedbackTransformation);
    }

}
