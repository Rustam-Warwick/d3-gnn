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
import functions.MovieLensStreamParser;
import functions.StreamingGNNLayerFunction;
import functions.TrainTestSplitter;
import functions.loss.BinaryCrossEntropy;
import functions.loss.EdgeLossFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import partitioner.HDRF;
import plugins.GNNLayerInference;
import plugins.GNNOutputEdgeInference;
import plugins.RandomNegativeSampler;
import storage.TupleStorage;

public class Main {
    public static void main(String[] args) throws Exception {
        GraphStream gs = new GraphStream((short) 3, (short) 2);
        DataStream<GraphOp> ratingsEdgeStream = gs.readSocket(new MovieLensStreamParser(), "localhost", 9090);
        DataStream<GraphOp> partitioned = gs.partition(ratingsEdgeStream, new HDRF());
        DataStream<GraphOp> splittedData = gs.trainTestSplit(partitioned, new TrainTestSplitter(0.01));
        DataStream<GraphOp> gnn1 = gs.gnnLayerNewIteration(splittedData, new StreamingGNNLayerFunction(new TupleStorage().withPlugin(new GNNLayerInference() {
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
        })));

        DataStream<GraphOp> nextLayerInput = gnn1.keyBy(new ElementIdSelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .evictor(CountEvictor.of(1))
                .apply(new WindowFunction<GraphOp, GraphOp, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<GraphOp> input, Collector<GraphOp> out) throws Exception {
                        for (GraphOp e : input) {
                            out.collect(e);
                        }
                    }
                });

        DataStream<GraphOp> gnn2 = gs.gnnLayerNewIteration(nextLayerInput, new StreamingGNNLayerFunction(new TupleStorage().withPlugin(new GNNLayerInference() {
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
        }).withPlugin(new RandomNegativeSampler(0.01))));

        DataStream<GraphOp> nextLayerInput2 = gnn2.keyBy(new ElementIdSelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .evictor(CountEvictor.of(1))
                .apply(new WindowFunction<GraphOp, GraphOp, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<GraphOp> input, Collector<GraphOp> out) throws Exception {
                        for (GraphOp e : input) {
                            out.collect(e);
                        }
                    }
                });


        DataStream<GraphOp> trainData = ((SingleOutputStreamOperator<GraphOp>) splittedData).getSideOutput(new OutputTag<>("training", TypeInformation.of(GraphOp.class)));
        DataStream<GraphOp> outputFunction = gs.gnnLayerNewIteration(nextLayerInput2.union(trainData), new StreamingGNNLayerFunction(new TupleStorage().withPlugin(new GNNOutputEdgeInference() {
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
