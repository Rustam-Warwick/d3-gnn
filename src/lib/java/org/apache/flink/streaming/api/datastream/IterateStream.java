package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.IterateTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.runtime.translators.IterateTransformationTranslator;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a Stream that has iterations.
 * Use this instead of default {@link IterativeStream}
 *
 * @param <T>  Output of the iteration body
 * @param <IT> Type of elements in the iteration, in other words input of the iteration body
 * @implNote This object can be inter-operably used with your body transformation {@link DataStream}. In other words the underlying transformations are the same
 */
public class IterateStream<T, IT> extends DataStream<T> {

    static {
        try {
            Field translatorMapField = StreamGraphGenerator.class.getDeclaredField("translatorMap");
            Field translatorModifiersField = translatorMapField.getClass().getDeclaredField("modifiers");
            translatorMapField.setAccessible(true);
            translatorModifiersField.setAccessible(true);
            translatorModifiersField.setInt(translatorMapField, translatorMapField.getModifiers() & ~Modifier.FINAL);
            Map<Class<? extends Transformation>, TransformationTranslator<?, ? extends Transformation>> translatorMap = new HashMap<>((Map) translatorMapField.get(null));
            translatorMap.putIfAbsent(IterateTransformation.class, new IterateTransformationTranslator<>());
            translatorMapField.set(null, Collections.unmodifiableMap(translatorMap));
            translatorModifiersField.setInt(translatorMapField, translatorMapField.getModifiers() & Modifier.FINAL);
            translatorMapField.setAccessible(false);
            translatorModifiersField.setAccessible(false);
        } catch (Exception e) {
            throw new RuntimeException("Can't find Translator Map, something is wrong. Try turning off the SecurityManager");
        }
    }

    /**
     * Reference to the main HEAD {@link IterateTransformation}
     */
    protected IterateTransformation<IT> headTransformation;

    private IterateStream(StreamExecutionEnvironment environment, Transformation<T> transformation, IterateTransformation<IT> headTransformation) {
        super(environment, transformation);
        this.headTransformation = headTransformation;
        environment.addOperator(headTransformation); // Need this otherwise nothing is going to point go through this operator
    }

    /**
     * Start the Iteration parallel to this element
     */
    public static <T, IT> IterateStream<T, IT> startIteration(DataStream<T> body) {
        Preconditions.checkState(body.getTransformation() instanceof PhysicalTransformation, "Iteration Body should be a physical operator");
        Preconditions.checkState(body.getTransformation().getInputs().size() > 0, "Iteration Body cannot be a source operator, apply an identity map to overcome this");
        return new IterateStream<>(body.getExecutionEnvironment(), body.getTransformation(), new IterateTransformation<>((PhysicalTransformation<T>) body.getTransformation()));
    }

    /**
     * Add Iteration Tail to the startTermination element that was created
     */
    public void closeIteration(DataStream<IT> feedbackStream) {
        headTransformation.addFeedbackEdge(feedbackStream.getTransformation());
    }

}
