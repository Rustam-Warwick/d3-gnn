package org.apache.flink.streaming.api.operators.graph;

import elements.GraphOp;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * <strong>SPLITTER</strong> operator located at the start of the operator chain
 * <p>
 *     Apart from taking in the {@link KeyedProcessFunction} for splitting the {@link datasets.Dataset}
 *     also handles the training and flushing the input stream
 * </p>
 */
public class DatasetSplitterOperator extends KeyedProcessOperator<PartNumber, GraphOp, GraphOp> {
    public DatasetSplitterOperator(KeyedProcessFunction<PartNumber, GraphOp, GraphOp> function, StreamOperatorParameters<GraphOp> parameters) {
        super(function);
        this.processingTimeService = parameters.getProcessingTimeService();
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

}
