package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import java.util.List;

public class WithIterationStreamGraphGenerator extends StreamGraphGenerator{
    public WithIterationStreamGraphGenerator(List<Transformation<?>> transformations, ExecutionConfig executionConfig, CheckpointConfig checkpointConfig) {
        super(transformations, executionConfig, checkpointConfig);
    }

    public WithIterationStreamGraphGenerator(List<Transformation<?>> transformations, ExecutionConfig executionConfig, CheckpointConfig checkpointConfig, ReadableConfig configuration) {
        super(transformations, executionConfig, checkpointConfig, configuration);
    }


}
