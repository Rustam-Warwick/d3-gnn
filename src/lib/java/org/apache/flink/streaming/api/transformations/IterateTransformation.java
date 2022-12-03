package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a Transformation for the IterationHead
 * @param <T> Output type that goes into the iteration
 */
public class IterateTransformation<T> extends Transformation<T> {

    /**
     * The original transformation to which this should be jointly sent
     */
    protected PhysicalTransformation<?> input;

    /**
     * All the transformations that are the tails for this transformation
     */
    protected Set<Transformation<T>> feedbackTransformations = new HashSet<>(3);

    public IterateTransformation(PhysicalTransformation<?> input) {
        super(String.format("%s[HEAD]", input.getName()), (TypeInformation<T>) (input.getInputs().isEmpty()?input.getOutputType():input.getInputs().get(0).getOutputType()), input.getParallelism());
        this.input = input;
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        return input.getTransitivePredecessors();
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return input.getInputs();
    }

    public void addFeedbackEdge(Transformation<?> feedbackTransformation){
        Preconditions.checkState(feedbackTransformation.getOutputType().equals(getOutputType()));
        feedbackTransformations.add((Transformation<T>) feedbackTransformation);
    }

}
