package aggregator.StreamingGNNAggregator;

import edge.SimpleEdge;
import org.tensorflow.Operand;
import org.tensorflow.op.Ops;
import org.tensorflow.op.core.Constant;
import org.tensorflow.op.core.Zeros;
import org.tensorflow.op.math.Add;
import org.tensorflow.op.nn.LeakyRelu;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TInt64;
import types.TFWrapper;
import vertex.SimpleVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class StreamingGNNAggregator extends BaseStreamingGNNAggregator<SimpleVertex, SimpleEdge, TFloat32> {

    public StreamingGNNAggregator() {
        super();
    }


    @Override
    public CompletableFuture<TFWrapper<TFloat32>> message(SimpleEdge e) {
        Ops op = Ops.create();
        return CompletableFuture.allOf(e.feature.getValue(),e.source.feature.getValue(),e.destination.hidden.getValue()).thenApply((b)->{
            TFloat32 source = e.source.feature.getValue().join().getValue();
            TFloat32 edge = e.feature.getValue().join().getValue();
            Constant<TFloat32> srT = op.constantOf(source);
            Constant<TFloat32> edT = op.constantOf(edge);
            Add<TFloat32> sm = op.math.add(srT,edT);
            LeakyRelu<TFloat32> output = op.nn.leakyRelu(sm);
//                OUTPUT OF MESSAGE FUNCTION
            Constant<TInt64> dims = op.constant(output.shape().asArray());
            Zeros<TFloat32> agg = op.zeros(dims,TFloat32.class);
            TFWrapper<TFloat32> agg_cur = e.destination.hidden.getValue().join();
            TFloat32 o;
            if(!Objects.isNull(agg_cur)){
                Constant<TFloat32> tmp = op.constantOf(agg_cur.getValue());
                List<Operand<TFloat32>> s = new ArrayList<Operand<TFloat32>>();
                s.add(agg);s.add(output);s.add(tmp);
                o = op.math.addN(s).asTensor();
            }else{
                o = op.math.add(agg,output).asTensor();
            }
            TFWrapper<TFloat32> ret= new TFWrapper<TFloat32>(o);
            e.destination.hidden.setValue(ret);
            return ret;
        });
    }

    @Override
    public CompletableFuture<TFWrapper<TFloat32>> update(SimpleVertex e) {
        return e.hidden.getValue();
    }

}
