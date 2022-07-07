package helpers;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.SerializableLoss;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.nn.gnn.SAGEConv;
import ai.djl.pytorch.engine.PtModel;
import ai.djl.training.loss.Loss;
import elements.GraphOp;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import plugins.ModelServer;
import plugins.edge_classification.EdgeClassificationTrainingPlugin;
import plugins.embedding_layer.GNNEmbeddingLayerTrainingPlugin;
import plugins.embedding_layer.StreamingGNNEmbeddingLayer;
import plugins.embedding_layer.WindowedGNNEmbeddingLayer;
import storage.FlatInMemoryClassStorage;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;

public class Main {
    public static ArrayList<Model> layeredModel() throws MalformedModelException, IOException {
        SequentialBlock sb = new SequentialBlock();
        sb.add(new SAGEConv(64, true));
        sb.add(new SAGEConv(32, true));
        sb.add(
                new SequentialBlock()
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                if(ndArrays.size() == 1){
                                    return new NDList(ndArrays.get(0).concat(ndArrays.get(0),-1));
                                }
                                return new NDList(ndArrays.get(0).concat(ndArrays.get(1),-1));
                            }
                        })
                        .add(Linear.builder().setUnits(16).optBias(true).build())
                        .add(Linear.builder().setUnits(1).optBias(true).build())
        );
        PtModel model = (PtModel) Model.newInstance("GNN");
        model.setBlock(sb);
//        NDHelper.loadModel(Path.of("/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/jupyter/models/GraphSageBias-2022-05-15"), model);
        model.getBlock().initialize(model.getNDManager(), DataType.FLOAT32, new Shape(64));
        model.getBlock().getParameters().forEach(item -> item.getValue().getArray().detach());
        ArrayList<Model> models = new ArrayList<>();
        sb.getChildren().forEach(item -> {
            PtModel tmp = (PtModel) Model.newInstance("GNN"); // Should all have the same name
            tmp.setBlock(item.getValue());
            models.add(tmp);
        });

        return models;
    }

    public static void main(String[] args) throws Exception {
        // Configuration
        Arrays.sort(args);
        ArrayList<Model> models = layeredModel();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // DataFlow
        Integer window = null;
        GraphStream gs = new GraphStream(env, args);
        DataStream<GraphOp>[] embeddings = gs.gnnEmbeddings(false, true, true,
                new StreamingGNNLayerFunction(new FlatInMemoryClassStorage()
                        .withPlugin(new ModelServer(models.get(0)))
                        .withPlugin(new GNNEmbeddingLayerTrainingPlugin(models.get(0).getName()))
                        .withPlugin(
                                window != null ?
                                        new WindowedGNNEmbeddingLayer(models.get(0).getName(), false, window) :
                                        new StreamingGNNEmbeddingLayer(models.get(0).getName(), true))
                ),
                new StreamingGNNLayerFunction(new FlatInMemoryClassStorage()
                        .withPlugin(new ModelServer(models.get(1)))
                        .withPlugin(new GNNEmbeddingLayerTrainingPlugin(models.get(1).getName()))
                        .withPlugin(
                                window != null ?
                                        new WindowedGNNEmbeddingLayer(models.get(0).getName(), true, 2 * window) :
                                        new StreamingGNNEmbeddingLayer(models.get(0).getName()))
                ),
                new StreamingGNNLayerFunction(new FlatInMemoryClassStorage()
                        .withPlugin(new ModelServer(models.get(2)))
                        .withPlugin(new EdgeClassificationTrainingPlugin(models.get(2).getName(), new SerializableLoss(Loss.l2Loss())))
                )
        );

        String timeStamp = new SimpleDateFormat("MM.dd.HH.mm").format(new java.util.Date());
        String jobName = String.format("%s (%s) [%s] %s", timeStamp, env.getParallelism(), String.join(" ", args), window == null ? "Streaming" : "Window-" + window);

//        SingleOutputStreamOperator<Void> outputStream = embeddings[embeddings.length - 1].forward().process(new ProcessFunction<GraphOp, Void>() {
//            @Override
//            public void processElement(GraphOp value, ProcessFunction<GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
//                // Do nothing just here to collect the metrics from previous operator
//            }
//        }).setParallelism(embeddings[embeddings.length - 1].getParallelism()).name("Embeddings");
////        SingleOutputStreamOperator<GraphOp> partitionedStream = embeddings[0].forward().process(new AddTimestamp()).setParallelism(1).name("Inputs");
////        SingleOutputStreamOperator<GraphOp> outputStream = embeddings[embeddings.length - 1].forward().process(new AddTimestamp()).setParallelism(embeddings[embeddings.length - 1].getParallelism()).name("Embeddings");
////        SingleOutputStreamOperator<Void> resultLatencies = partitionedStream.keyBy(GraphOp::getTimestamp).connect(outputStream.keyBy(GraphOp::getTimestamp)).process(new LatencyOutput(jobName, 10000)).name("Latencies").setParallelism(embeddings[embeddings.length - 1].getParallelism());
//        if (embeddings[embeddings.length - 1].getTransformation().getSlotSharingGroup().isPresent()) {
//            outputStream.slotSharingGroup(embeddings[embeddings.length - 1].getTransformation().getSlotSharingGroup().get());
////            resultLatencies.slotSharingGroup(embeddings[embeddings.length - 1].getTransformation().getSlotSharingGroup().get());
//        }
////        if (embeddings[0].getTransformation().getSlotSharingGroup().isPresent()) {
////            partitionedStream.slotSharingGroup(embeddings[0].getTransformation().getSlotSharingGroup().get());
////        }

        env.execute(jobName);
    }
}
