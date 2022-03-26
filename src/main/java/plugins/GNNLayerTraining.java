package plugins;

import aggregators.BaseAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.Parameter;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import features.VTensor;
import helpers.JavaTensor;
import iterations.IterationState;
import iterations.RemoteDestination;
import iterations.RemoteFunction;
import iterations.Rpc;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class GNNLayerTraining extends Plugin {
    public GNNLayerInference inference;
    public int collectedGradsSoFar = 0; // Master node collected gradients count
    public GNNLayerTraining(){
        super("trainer");
    }

    @Override
    public void onWatermark(Watermark w) {
//        System.out.println("Update sent: "+ storage.operatorIndex+" Position: "+ storage.position+" To Master " + masterPart());
        Rpc.callProcedure(this, "collectGradients", IterationState.ITERATE, RemoteDestination.MASTER, this.inference.parameterStore.gradientArrays);
    }

    @Override
    public void open() {
        super.open();
        inference = (GNNLayerInference) this.storage.getPlugin("inferencer");
    }

    /**
     * Backward trigger function
     * @param grad grad to be passed for VJP
     */
    @RemoteFunction
    public void backward(VTensor grad){
        // 1. Get Data
        grad.setStorage(this.storage);
        VTensor feature = (VTensor) grad.getElement().getFeature("feature");
        BaseAggregator<?> agg = (BaseAggregator<?>) grad.getElement().getFeature("agg");
        feature.getValue().setRequiresGradient(true);
        agg.getValue().setRequiresGradient(true);

        // 2. Prediction & Backward
        NDArray prediction = this.inference.update(feature.getValue(), agg.getValue(), true);
        JniUtils.backward((PtNDArray) prediction, (PtNDArray) grad.getValue(), false, false);

        // 3. Send Update backward if this is not last layer
        if(!this.storage.isFirst()){
            grad.value = new Tuple2<>(feature.getValue().getGradient(), 0);
            Rpc backward = new Rpc("trainer", "backward", new Object[]{grad}, ElementType.PLUGIN, false);
            this.storage.message(new GraphOp(Op.RPC, this.storage.currentKey, backward, IterationState.BACKWARD));
        }

        // 4. Send to messageBackward to do the message backward steps

        grad.value = new Tuple2<>(agg.grad(), 0);
        Rpc.callProcedure(this, "messageBackward", IterationState.ITERATE, agg.replicaParts(), grad);
        this.messageBackward(grad);

        // 5. Cleanup
        agg.getValue().setRequiresGradient(false);
        feature.getValue().setRequiresGradient(false);
    }

    /**
     * Backward step for the message function
     * @param aggGrad grad of message output w.r.t loss
     */
    @RemoteFunction
    public void messageBackward(VTensor aggGrad){
        aggGrad.setStorage(this.storage);
        Vertex vertex = (Vertex) aggGrad.getElement();
        Iterable<Edge> inEdges = this.storage.getIncidentEdges(vertex, EdgeType.IN);
        for(Edge edge: inEdges){
            if(this.inference.messageReady(edge)){
                NDArray inFeature = (NDArray) edge.src.getFeature("feature").getValue();
                inFeature.setRequiresGradient(true);
                NDArray prediction = this.inference.message(inFeature, true);
                JniUtils.backward((PtNDArray) prediction, (PtNDArray) aggGrad.getValue(), false, false);
                if(!this.storage.isFirst()){
                    VTensor grad = new VTensor("grad", new Tuple2<>(inFeature.getGradient(), 0));
                    grad.attachedTo = new Tuple2<>(edge.src.elementType(), edge.src.getId());
                    Rpc backward = new Rpc("trainer", "backward", new Object[]{grad}, ElementType.PLUGIN, false);
                    this.storage.message(new GraphOp(Op.RPC, edge.src.masterPart(), backward, IterationState.BACKWARD));
                }
                ((NDArray) edge.src.getFeature("feature").getValue()).setRequiresGradient(false);
            }
        }
    }

    /**
     * Accumulates all the gradients in master operator
     * @param grads
     */
    @RemoteFunction
    public void collectGradients(Map<String, NDArray> grads){
        this.inference.parameterStore.addGrads(grads);
        collectedGradsSoFar++;
//        System.out.println("Gradient Received: "+ storage.operatorIndex+" Position: "+ storage.position +" So far received: "+collectedGradsSoFar);
        if(collectedGradsSoFar == replicaParts().size() + 1){
            collectedGradsSoFar = 0;
            this.inference.parameterStore.updateAllParameters();
            Rpc.callProcedure(this, "updateParameters", IterationState.ITERATE, RemoteDestination.REPLICAS, this.inference.parameterStore.parameterArrays);
            this.updateParameters(this.inference.parameterStore.parameterArrays);
        }
    }

    /**
     * Given new parameters synchronize them across the parallel instances
     * @param params
     */
    @RemoteFunction
    public void updateParameters(Map<String, NDArray> params){
        this.inference.parameterStore.updateParameters(params);
        this.inference.parameterStore.resetGrads();
        this.inference.MODEL_VERSION++;
        System.out.println("Parameters Updated: "+ storage.operatorIndex + "position: "+ storage.position + " Model version " + inference.MODEL_VERSION);
        Rpc.callProcedure(this, "reInference", IterationState.ITERATE, this.storage.keys);
    }

    /**
     * New Parameters have been committed, need to increment the model version
     */
    @RemoteFunction
    public void reInference(){
        Iterable<Vertex> vertices = this.storage.getVertices();
        for(Vertex v: vertices){
            this.inference.reduceInEdges(v);
        }
    }



}
