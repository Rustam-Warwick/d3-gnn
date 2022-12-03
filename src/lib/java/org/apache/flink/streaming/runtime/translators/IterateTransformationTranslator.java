package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.iteration.IterationHeadOperatorFactory;
import org.apache.flink.streaming.api.transformations.IterateTransformation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IterateTransformationTranslator<OUT> implements TransformationTranslator<OUT, IterateTransformation<OUT>> {
    private static Logger LOG = LoggerFactory.getLogger(IterateTransformationTranslator.class);
    @Override
    public Collection<Integer> translateForBatch(IterateTransformation<OUT> transformation, Context context) {
        throw new IllegalStateException("Batch Transformations are not allowed yet");
    }

    @Override
    public Collection<Integer> translateForStreaming(IterateTransformation<OUT> transformation, Context context) {
        Preconditions.checkNotNull(transformation.getIterationStartTransformation());
        if(transformation.getIterationTailTransformations().isEmpty()){
            // If no tails do not add any iteration stuff
            return Collections.emptyList();
        }
        final StreamGraph streamGraph = context.getStreamGraph();
        final String slotSharingGroup = context.getSlotSharingGroup();
        final int iterationHeadId = transformation.getId();
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

        // First Transform the IterationStartTransformations and IterationEndTransformations
        Collection<Integer> startVertexIds = context.transform(transformation.getIterationStartTransformation());
        Collection<Integer> endVertexIds = transformation.getIterationTailTransformations().stream().map(context::transform).flatMap(item->(Stream<Integer>) item.stream()).collect(Collectors.toList());
        StreamNode startStreamNode = startVertexIds.stream().map(id -> context.getStreamGraph().getStreamNode(id)).collect(Collectors.toList()).get(0);
        List<StreamNode> endStreamNodes = endVertexIds.stream().map(id -> context.getStreamGraph().getStreamNode(id)).collect(Collectors.toList());

        // Add the Iteration HEAD Operator
        streamGraph.addLegacySource(
                iterationHeadId,
                slotSharingGroup,
                transformation.getCoLocationGroupKey(),
                new IterationHeadOperatorFactory<>(iterationHeadId),
                transformation.getOutputType(),
                transformation.getOutputType(),
                String.format("[HEAD]%s",startStreamNode.getOperatorName()));

        // Add the Iteration TAIL Operators

        // Make Edges
        streamGraph.addEdge();

        // Configure parallelism slotSharingGroup and coLocationGroups

        streamGraph.setParallelism(iterationHeadId, 10);
        streamGraph.setMaxParallelism(iterationHeadId, 10);
//        );
//        streamGraph.addOperator(
//                transformationId,
//                slotSharingGroup,
//                transformation.getCoLocationGroupKey(),
//                operatorFactory,
//                inputType,
//                transformation.getOutputType(),
//                transformation.getName());
//
//        int parallelism =
//                transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
//                        ? transformation.getParallelism()
//                        : executionConfig.getParallelism();
//        streamGraph.setParallelism(transformationId, parallelism);
//        streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());
//
//        final List<Transformation<?>> parentTransformations = transformation.getInputs();
//        checkState(
//                parentTransformations.size() == 1,
//                "Expected exactly one input transformation but found "
//                        + parentTransformations.size());
//
//        for (Integer inputId : context.getStreamNodeIds(parentTransformations.get(0))) {
//            streamGraph.addEdge(inputId, transformationId, 0);
//        }

        return Collections.singleton(iterationHeadId);
    }
}
