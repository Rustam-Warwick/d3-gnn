package sources;

import edge.SimpleEdge;
import features.ReplicableTensorFeature;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import types.GraphQuery;
import vertex.SimpleVertex;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class TSVFileStreamer extends RichSourceFunction<GraphQuery> {
    String fileName;
    transient BufferedReader dataReader=null;
    public TSVFileStreamer(String fileName) throws FileNotFoundException {
        super();
        this.fileName = fileName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.dataReader = new BufferedReader(new FileReader(fileName));
    }

    @Override
    public void run(SourceContext<GraphQuery> ctx) throws Exception {
        String line = null;
        int count = 0;
        while((line=this.dataReader.readLine())!=null){
            System.out.println(++count);
            String[] lineItems = line.split("\t");
            String id1 = lineItems[0];
            String id2 = lineItems[1];
            SimpleVertex v1 = new SimpleVertex(id1);
            SimpleVertex v2 = new SimpleVertex(id2);
            INDArray v1A = Nd4j.ones(1024);
            v1.feature = new ReplicableTensorFeature("feature",v1,v1A);
            v2.feature = new ReplicableTensorFeature("feature",v2,v1A);
            SimpleEdge<SimpleVertex> ed = new SimpleEdge<>(v1,v2);
            SimpleEdge<SimpleVertex> ed2 = new SimpleEdge<>(v2,v1);
            GraphQuery q = new GraphQuery(ed).changeOperation(GraphQuery.OPERATORS.ADD);
            GraphQuery q2 = new GraphQuery(ed2).changeOperation(GraphQuery.OPERATORS.ADD);
            ctx.collect(q);
//            ctx.collect(q2);
        }
        System.out.println("FINISHED\n\n\n\n");
    }

    @Override
    public void cancel() {
       try{
           this.dataReader.close();
       }catch (Exception e){

       }
    }
}
