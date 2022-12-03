package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.IterateTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.util.Preconditions;

/**
 * Represents a Stream that has iterations
 * Use this instead of default {@link IterativeStream}
 * @implNote This object can be interoperably used with your main transformation {@link DataStream}
 */
public class IterateStream<T> extends DataStream<T>{

    /**
     * Reference to the main HEAD {@link IterateTransformation}
     */
    protected IterateTransformation<?> headTransformation;

    private IterateStream(StreamExecutionEnvironment environment, Transformation<T> transformation, IterateTransformation<?> headTransformation) {
        super(environment, transformation);
        this.headTransformation = headTransformation;
        environment.addOperator(headTransformation);
    }

    public static <T> IterateStream<T> startIteration(DataStream<T> input){
        Preconditions.checkState(input.getTransformation() instanceof PhysicalTransformation);
        return new IterateStream<>(input.getExecutionEnvironment(), input.getTransformation(), new IterateTransformation<>((PhysicalTransformation<?>) input.getTransformation()));
    }

    public void closeIteration(DataStream<?> feedbackStream){
        headTransformation.addFeedbackEdge(feedbackStream.getTransformation());
    }

}
