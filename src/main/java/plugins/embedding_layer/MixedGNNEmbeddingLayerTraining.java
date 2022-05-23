package plugins.embedding_layer;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import elements.Plugin;
import elements.ReplicaState;
import elements.iterations.RemoteFunction;

import java.util.HashMap;

public class MixedGNNEmbeddingLayerTraining extends Plugin {
    public transient MixedGNNEmbeddingLayer inference;
    public int numOutputChannels;
    public int synchronizeMessages;


    public MixedGNNEmbeddingLayerTraining() {
        super("trainer");
    }

    @Override
    public void open() {
        super.open();
        inference = (MixedGNNEmbeddingLayer) this.storage.getPlugin("inferencer");
        numOutputChannels = storage.layerFunction.getNumberOfOutChannels(null);
    }

    @RemoteFunction
    public void collect(HashMap<String, NDArray> gradients){
        Feature<?,?> feature = storage.getFeature("collectedGradients");
        if(feature==null) storage.addFeature(new Feature("collectedGradients", gradients, true, getPartId()));
        else {
            Feature<HashMap<String, NDArray>, HashMap<String, NDArray>> collectedGradients = (Feature<HashMap<String, NDArray>, HashMap<String, NDArray>>) feature;
            gradients.forEach((key, grad)->{
                collectedGradients.getValue().computeIfPresent(key, (s, item)->(item.add(grad)));
                collectedGradients.getValue().putIfAbsent(key, grad);
            });
            storage.updateFeature(collectedGradients);
        }
    }

    public void startTraining(){

    }

    @RemoteFunction
    public void synchronize(){
        if(state()== ReplicaState.MASTER){
            ++synchronizeMessages;
        }
        if(synchronizeMessages == numOutputChannels){
            startTraining();
        }

    }

}
