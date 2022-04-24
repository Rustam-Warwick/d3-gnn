package helpers;

import ai.djl.Model;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.LambdaBlock;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.training.loss.Loss;
import elements.GraphOp;
import features.VTensor;
import functions.loss.BinaryCrossEntropy;
import functions.loss.ClassificationLossFunction;
import functions.loss.EdgeLossFunction;
import functions.nn.MyActivations;
import functions.parser.CoraMergedStreamParser;
import functions.StreamingGNNLayerFunction;
import functions.parser.MovieLensStreamParser;
import functions.splitter.EdgeTrainTestSplitter;
import functions.splitter.FeaturedVertexSplitter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import partitioner.HDRF;
import plugins.edge_detection.EdgeOutputInference;
import plugins.gnn_layer.GNNLayerInference;
import plugins.RandomNegativeSampler;
import plugins.vertex_classification.VertexOutputInference;
import storage.TupleStorage;

import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        GraphStream gs = new GraphStream((short) 3);
        DataStream<GraphOp> ratingsEdgeStream = gs.readSocket(new MovieLensStreamParser(), "localhost", 9090);
        DataStream<GraphOp> partitioned = gs.partition(ratingsEdgeStream, new HDRF());
        DataStream<GraphOp> splittedData = gs.trainTestSplit(partitioned, new EdgeTrainTestSplitter(0.005));

        DataStream<GraphOp> embeddings = gs.gnnEmbeddings(splittedData, List.of(
                new StreamingGNNLayerFunction(new TupleStorage().withPlugin(new GNNLayerInference(false) {
                    @Override
                    public Model createMessageModel() {
                        SequentialBlock myBlock = new SequentialBlock();
                        myBlock.add(Linear.builder().setUnits(32).build());
                        myBlock.add(Activation::relu);
                        myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7));
                        Model model = Model.newInstance("inference");
                        model.setBlock(myBlock);
                        return model;
                    }

                    @Override
                    public Model createUpdateModel() {
                        SequentialBlock myBlock = new SequentialBlock();
                        myBlock.add(new LambdaBlock(inputs -> (
                                new NDList(inputs.get(0).concat(inputs.get(1)))
                        )));
                        myBlock.add(Activation::relu);
                        myBlock.add(Linear.builder().setUnits(16).build());
                        myBlock.add(Activation::relu);
                        myBlock.add(Linear.builder().setUnits(7).build());
                        myBlock.add(Activation::relu);
                        myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7), new Shape(32));
                        Model model = Model.newInstance("message");
                        model.setBlock(myBlock);
                        return model;
                    }
                })),
                new StreamingGNNLayerFunction(new TupleStorage().withPlugin(new GNNLayerInference(true) {
                    @Override
                    public Model createMessageModel() {
                        SequentialBlock myBlock = new SequentialBlock();
                        myBlock.add(Linear.builder().setUnits(32).build());
                        myBlock.add(Activation::relu);
                        myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7));
                        Model model = Model.newInstance("inference");
                        model.setBlock(myBlock);
                        return model;
                    }

                    @Override
                    public Model createUpdateModel() {
                        SequentialBlock myBlock = new SequentialBlock();
                        myBlock.add(new LambdaBlock(inputs -> (
                                new NDList(inputs.get(0).concat(inputs.get(1)))
                        )));
                        myBlock.add(Activation::relu);
                        myBlock.add(Linear.builder().setUnits(16).build());
                        myBlock.add(Activation::relu);
                        myBlock.add(Linear.builder().setUnits(7).build());
                        myBlock.add(Activation::relu);
                        myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7), new Shape(32));
                        Model model = Model.newInstance("message");
                        model.setBlock(myBlock);
                        return model;
                    }
                }).withPlugin(new RandomNegativeSampler(0.003)))
        ));
        DataStream<GraphOp> trainData = ((SingleOutputStreamOperator<GraphOp>) splittedData).getSideOutput(new OutputTag<>("training", TypeInformation.of(GraphOp.class)));
        DataStream<GraphOp> outputFunction = gs.gnnLayerNewIteration(embeddings.union(trainData), new StreamingGNNLayerFunction(new TupleStorage().withPlugin(new EdgeOutputInference() {
            @Override
            public Model createOutputModel() {
                SequentialBlock myBlock = new SequentialBlock();
                myBlock.add(new LambdaBlock(inputs -> (
                        new NDList(inputs.get(0).concat(inputs.get(1)))
                )));
                myBlock.add(Linear.builder().setUnits(16).build());
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(32).build());
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(1).build());
                myBlock.add(Activation::sigmoid);
                myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7), new Shape(7));
                Model model = Model.newInstance("prediction");
                model.setBlock(myBlock);
                return model;
            }
        })));
//
        DataStream<GraphOp> trainPredictionsData = ((SingleOutputStreamOperator<GraphOp>) outputFunction).getSideOutput(new OutputTag<>("training", TypeInformation.of(GraphOp.class)));
        gs.gnnLoss(trainPredictionsData, new EdgeLossFunction(120) {
            @Override
            public Loss createLossFunction() {
                return new BinaryCrossEntropy();
            }
        });


        gs.env.execute();
    }
}
