package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.IterateTransformation;

import java.util.Collection;

public class IterateTransformationTranslator<OUT> implements TransformationTranslator<OUT, IterateTransformation<OUT>> {

    @Override
    public Collection<Integer> translateForBatch(IterateTransformation<OUT> transformation, Context context) {
        throw new IllegalStateException("Batch Transformations are not allowed yet");
    }

    @Override
    public Collection<Integer> translateForStreaming(IterateTransformation<OUT> transformation, Context context) {
        return null;
    }
}
