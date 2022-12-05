package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.iteration.IterationHeadOperatorFactory;
import org.apache.flink.streaming.api.operators.iteration.IterationTailOperatorFactory;
import org.apache.flink.streaming.api.transformations.IterateTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class IterateTransformationTranslator<OUT> extends SimpleTransformationTranslator<OUT, IterateTransformation<OUT>> {
    {
        try{
            Field operatorFactoryField = StreamNode.class.getDeclaredField("operatorFactory");

        }catch (Exception e){
            throw new RuntimeException("Cannot use Reflection turn off the Secrity Manager")
        }

    }
    private static Logger LOG = LoggerFactory.getLogger(IterateTransformationTranslator.class);

    @Override
    protected Collection<Integer> translateForBatchInternal(IterateTransformation<OUT> transformation, Context context) {
        throw new IllegalStateException("Batch Transformations are not allowed yet");
    }

    @Override
    public Collection<Integer> translateForStreamingInternal(IterateTransformation<OUT> transformation, Context context) {
        // Basic Error Handling
        Preconditions.checkNotNull(transformation.getIterationBodyTransformation());
        if(transformation.getIterationFeedbackTransformations().isEmpty()){
            // If no tails do not add any iteration stuff
            return Collections.emptyList();
        }
        // Easy Access
        final StreamGraph streamGraph = context.getStreamGraph();
        final int iterationHeadId = transformation.getId();
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();
        final List<Integer> results = new ArrayList<>();

        // Configuration of SlotSharingGroup And transforming body first
        final Collection<Integer> bodyVertexIds = context.transform(transformation.getIterationBodyTransformation()); // Create body transformation first
        Preconditions.checkState(bodyVertexIds.size() == 1, "Body of the Iteration should be a single operator, this transformation has multiple IDs");
        final StreamNode bodyStreamNode = context.getStreamGraph().getStreamNode(bodyVertexIds.iterator().next());
        final String coLocationGroupKey = bodyStreamNode.getCoLocationGroup() == null?String.format("Iteration-%s", iterationHeadId):bodyStreamNode.getCoLocationGroup();
        final String slotSharingGroup = bodyStreamNode.getSlotSharingGroup();
        bodyStreamNode.setCoLocationGroup(coLocationGroupKey);
        // Add HEAD Logic to the Operator
        streamGraph.addOperator(
                iterationHeadId,
                slotSharingGroup,
                coLocationGroupKey,
                new IterationHeadOperatorFactory<>(iterationHeadId),
                transformation.getOutputType(),
                transformation.getOutputType(),
                String.format("[HEAD]%s",bodyStreamNode.getOperatorName()));
        streamGraph.setParallelism(iterationHeadId, bodyStreamNode.getParallelism());
        streamGraph.setMaxParallelism(iterationHeadId, transformation.getIterationBodyTransformation().getMaxParallelism());
        streamGraph.addEdge(iterationHeadId, bodyStreamNode.getId(),0);
        // Add the Iteration TAIL Operators
        for (Transformation<OUT> iterationFeedbackTransformation : transformation.getIterationFeedbackTransformations()) {
            Collection<Integer> feedbackVertexIds = context.transform(iterationFeedbackTransformation); // Create feedback transformations first
            int iterationTailId = Transformation.getNewNodeId();
            results.add(iterationTailId);
            streamGraph.addOperator(
                    iterationTailId,
                    slotSharingGroup,
                    coLocationGroupKey,
                    new IterationTailOperatorFactory(iterationHeadId),
                    transformation.getOutputType(),
                    TypeExtractor.createTypeInfo(Void.class),
                    String.format("[TAIL]%s",bodyStreamNode.getOperatorName()));
            streamGraph.setParallelism(iterationTailId, bodyStreamNode.getParallelism());
            streamGraph.setMaxParallelism(iterationTailId, transformation.getIterationBodyTransformation().getMaxParallelism());
            if(bodyStreamNode.getStatePartitioners().length > 0){
                Preconditions.checkState(iterationFeedbackTransformation instanceof PartitionTransformation, "IterationBody and Feedback should be identically partitioned");
                streamGraph.setOneInputStateKey(iterationTailId, bodyStreamNode.getStatePartitioners()[0], bodyStreamNode.getStateKeySerializer().duplicate());
            }

            for (Integer feedbackVertexId : feedbackVertexIds) {
                streamGraph.addEdge(feedbackVertexId, iterationTailId, 0);
            }
        }
        return results;
    }
}
