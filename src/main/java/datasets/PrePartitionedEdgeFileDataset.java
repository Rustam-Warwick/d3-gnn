package datasets;

import elements.DirectedEdge;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.Op;
import it.unimi.dsi.fastutil.shorts.ShortList;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.Collector;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class PrePartitionedEdgeFileDataset extends Dataset{

    protected String fileName;
    @Override
    public boolean isResponsibleFor(String datasetName) {
        fileName = datasetName;
        return datasetName.contains("-partitioned-");
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String filePath = Path.of(System.getenv("DATASET_DIR"), String.format("%s.csv", fileName)).toString();
        SingleOutputStreamOperator<String> fileStream = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(filePath)), filePath, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000).name(fileName).setParallelism(1);
        return fileStream.map(new Parser()).name(String.format("Parser %s", fileName)).setParallelism(1);
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> getSplitter() {
        return new TrainTestSplitter();
    }

    /**
     * Actual Splitter function
     */
    protected static class TrainTestSplitter extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> {
        @Override
        public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            out.collect(value);
            ctx.output(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT, value); // Edge with Features even for the topology
        }
    }

    protected static class Parser extends RichMapFunction<String, GraphOp>{

        public transient Map<String, Short> partitionTable;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            partitionTable = new HashMap<>();
        }

        @Override
        public GraphOp map(String value) throws Exception {
            String[] values = value.split("\t");
            short partId = Short.parseShort(values[2]);
            DirectedEdge directedEdge = new DirectedEdge(new Vertex(values[0]), new Vertex(values[1]));
            partitionTable.putIfAbsent(directedEdge.getSrcId(), partId);
            partitionTable.putIfAbsent(directedEdge.getDestId(), partId);
            directedEdge.getSrc().masterPart = partitionTable.get(directedEdge.getSrcId());
            directedEdge.getDest().masterPart = partitionTable.get(directedEdge.getDestId());
            return new GraphOp(Op.ADD, partId, directedEdge);

        }
    }

}
