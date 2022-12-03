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
 * @implNote This object can be interoperably used with your main transformation {@link DataStream}
 * @param <T> Output of the head transformation
 * @param <IT> Input of the head transformation, or in other words the type of iteration elements
 */
public class IterateStream<T,IT> extends DataStream<T>{

    static{
        try{
            Field translatorMapField = StreamGraphGenerator.class.getDeclaredField("translatorMap");
            Field translatorModifiersField = translatorMapField.getClass().getDeclaredField("modifiers");
            translatorMapField.setAccessible(true);
            translatorModifiersField.setAccessible(true);
            translatorModifiersField.setInt(translatorMapField, translatorMapField.getModifiers() & ~Modifier.FINAL);
            Map< Class<? extends Transformation>,TransformationTranslator<?, ? extends Transformation>> translatorMap = new HashMap<>((Map) translatorMapField.get(null));
            translatorMap.putIfAbsent(IterateTransformation.class, new IterateTransformationTranslator<>());
            translatorMapField.set(null, Collections.unmodifiableMap(translatorMap));
            translatorModifiersField.setInt(translatorMapField, translatorMapField.getModifiers() & Modifier.FINAL);
            translatorMapField.setAccessible(false);
            translatorModifiersField.setAccessible(false);
        }catch (Exception e){
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
        environment.addOperator(headTransformation);
    }

    /**
     * Start the Iteration parallel to this element
     */
    public static <T,IT> IterateStream<T,IT> startIteration(DataStream<T> input){
        Preconditions.checkState(input.getTransformation() instanceof PhysicalTransformation);
        return new IterateStream<>(input.getExecutionEnvironment(), input.getTransformation(), new IterateTransformation<>((PhysicalTransformation<IT>) input.getTransformation()));
    }

    /**
     * Add Iteration Tail to the start element that was created
     */
    public void closeIteration(DataStream<IT> feedbackStream){
        headTransformation.addFeedbackEdge(feedbackStream.getTransformation());
    }

}
