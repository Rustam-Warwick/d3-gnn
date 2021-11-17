package sources;
import edge.SimpleEdge;
import features.ReplicableTensorFeature;
import features.StringReplicableFeature;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.nd4j.linalg.factory.Nd4j;
import types.GraphQuery;
import vertex.SimpleVertex;

import java.util.Random;
import java.util.function.IntConsumer;

/**
 * Responsible for fake graph generation.
 * Streaming Edges for a directed graph to be consumed by the main partitioner
 */
public class GraphGenerator extends RichParallelSourceFunction<GraphQuery> {
    private Random random;
    private volatile boolean isRunning = true;
    private int N; // Num of vertices
    private int D; // Average degree(in+out) per vertex
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        random = new Random();
        this.N = 5;
        this.D  = 2;

    }

    public boolean streamVertex(short value,double p){
        if(value>0){
            double coin = Math.random();
            return coin <= p;
        }
        return false;
    }
    @Override
    public void run(SourceContext<GraphQuery> ctx) throws Exception {
        short[] edges = new short[N];
        double pVertexStream = (double) 1/D;
        random.ints(N*D,0,N).forEach(new IntConsumer() {
            Integer srcId = null;
            @Override
            public void accept(int value) {
                if(!isRunning)throw new NullPointerException(); // Running is stopped

                if(srcId==null){
                    srcId = value; // Store the value wait for the next iteration
                }
                else{
                    // 1. Add as the source
                    SimpleVertex source = new SimpleVertex(srcId.toString());
                    source.feature = new ReplicableTensorFeature("feature",source,Nd4j.rand(2,2));
                    SimpleVertex destination = new SimpleVertex(String.valueOf(value));
                    destination.feature = new ReplicableTensorFeature("feature",destination,Nd4j.rand(2,2));
                    SimpleEdge<SimpleVertex> edge = new SimpleEdge<>(source,destination);
                    GraphQuery a = new GraphQuery(edge).changeOperation(GraphQuery.OPERATORS.ADD);
                    ctx.collect(a);
                    // 2. Increment data structure
                    edges[value]++;
                    edges[srcId]++;
                    this.srcId = null;
                }
            }
        });

        while(isRunning){
            // This part is needed since the jobs will close once this source function returns
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}

