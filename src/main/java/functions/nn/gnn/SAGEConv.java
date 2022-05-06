package functions.nn.gnn;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.core.Linear;
import ai.djl.training.ParameterStore;

public class SAGEConv extends GNNBlock{
    Linear selfForward;
    Linear neighForward;
    int outFeatures;

    public SAGEConv(int outFeatures) {
        this.selfForward = Linear.builder().setUnits(outFeatures).optBias(true).build();
        this.neighForward = Linear.builder().setUnits(outFeatures).optBias(true).build();
        this.addChildBlock("self_forward", selfForward);
        this.addChildBlock("neigh_forward", neighForward);
        this.outFeatures = outFeatures;
    }

    @Override
    public NDList message(ParameterStore parameterStore, NDList inputs, boolean training) {
        return inputs;
    }

    @Override
    public NDList update(ParameterStore parameterStore, NDList inputs, boolean training) {
        NDArray X_self = selfForward.forward(parameterStore, new NDList(inputs.get(0)), training).get(0);
        NDArray X_neigh = neighForward.forward(parameterStore, new NDList(inputs.get(1)), training).get(0);
        return new NDList(X_self.add(X_neigh));
    }

    @Override
    public Shape[] getOutputShapes(Shape[] inputShapes) {
        return new Shape[0];
    }
}
