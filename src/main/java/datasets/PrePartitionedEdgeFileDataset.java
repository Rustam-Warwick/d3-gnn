package datasets;

import elements.DirectedEdge;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.Op;
import it.unimi.dsi.fastutil.objects.Object2ShortOpenHashMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.Collector;

import java.nio.file.Path;

public class PrePartitionedEdgeFileDataset extends Dataset {

    protected String fileName;

    @Override
    public boolean isResponsibleFor(String datasetName) {
        fileName = datasetName;
        return datasetName.contains("-partitioned-");
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String filePath = Path.of(System.getenv("DATASET_DIR"), String.format("%s.csv", fileName)).toString();
        if (fileName.startsWith("s3")) {
            filePath = String.format("%s.csv", fileName);
        }
        if (fileName.contains("both-partitioned-")) {
            SingleOutputStreamOperator<String> fileStream = env.fromSource(FileSource.forRecordStreamFormat(new TextLineInputFormat("UTF-8"), new org.apache.flink.core.fs.Path(filePath)).build(), WatermarkStrategy.noWatermarks(), fileName).setParallelism(1);
            return fileStream.flatMap(new BothParser()).name("Parser " + fileName).setParallelism(1);
        }
        SingleOutputStreamOperator<String> fileStream = env.fromSource(FileSource.forRecordStreamFormat(new TextLineInputFormat("UTF-8"), new org.apache.flink.core.fs.Path(filePath)).build(), WatermarkStrategy.noWatermarks(), fileName).setParallelism(1);
        return fileStream.flatMap(new Parser()).name("Parser " + fileName).setParallelism(1);
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

    protected static class BothParser implements FlatMapFunction<String, GraphOp> {
        @Override
        public void flatMap(String value, Collector<GraphOp> out) {
            try {
                String[] srcDestPart = value.split(",");
                short partSrc = Short.parseShort(srcDestPart[2]);
                short partDest = Short.parseShort(srcDestPart[3]);
                DirectedEdge directedEdge = new DirectedEdge(new Vertex(srcDestPart[0]), new Vertex(srcDestPart[1]));
                directedEdge.getSrc().masterPart = partSrc;
                directedEdge.getDest().masterPart = partDest;
                GraphOp graphOp = new GraphOp(Op.ADD, partSrc, directedEdge);
                out.collect(graphOp);
            } catch (NumberFormatException e) {
                // Do nothing
            }
        }
    }

    /**
     * Map function that does not actually surve any purpose apart from partitioning the original stream the format
     * Actual parsing is done in the splitter
     */
    protected static class Parser extends RichFlatMapFunction<String, GraphOp> {

        protected transient Object2ShortOpenHashMap<String> partitionTable;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            partitionTable = new Object2ShortOpenHashMap<>(1000);
        }

        @Override
        public void flatMap(String value, Collector<GraphOp> out) {
            try {
                String[] srcDestPart = value.split(",");
                short part = Short.parseShort(srcDestPart[2]);
                DirectedEdge directedEdge = new DirectedEdge(new Vertex(srcDestPart[0]), new Vertex(srcDestPart[1]));
                partitionTable.putIfAbsent(directedEdge.getSrcId(), part);
                partitionTable.putIfAbsent(directedEdge.getDestId(), part);
                directedEdge.getSrc().masterPart = partitionTable.get(directedEdge.getSrcId());
                directedEdge.getDest().masterPart = partitionTable.get(directedEdge.getDestId());
                GraphOp graphOp = new GraphOp(Op.ADD, part, directedEdge);
                out.collect(graphOp);
            } catch (NumberFormatException e) {
                // Do nothing
            }
        }
    }


}
