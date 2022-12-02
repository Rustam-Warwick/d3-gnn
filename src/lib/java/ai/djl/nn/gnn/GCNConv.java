package ai.djl.nn.gnn;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Parameter;
import ai.djl.nn.core.Linear;
import ai.djl.training.ParameterStore;

public final class GCNConv extends GNNBlock {
    Linear self;

    Parameter bias;

    Shape outputShape;

    public GCNConv(int outFeatures, boolean optBias) {
        if (optBias) {
            bias = Parameter.builder().setName("bias").setType(Parameter.Type.BIAS).build();
            addParameter(bias);
        }
        this.self = Linear.builder().setUnits(outFeatures).optBias(false).build();
        addChildBlock("self", self);
        outputShape = new Shape(outFeatures);
    }

    @Override
    public void initialize(NDManager manager, DataType dataType, Shape... inputShapes) {
        beforeInitialize(inputShapes);
        this.self.initialize(manager, dataType, inputShapes);
        if (bias != null && !bias.isInitialized()) {
            bias.setShape(outputShape);
            bias.initialize(manager, dataType);
        }
    }

    @Override
    public NDList message(ParameterStore parameterStore, NDList inputs, boolean training) {
        return inputs;
    }

    @Override
    public NDList update(ParameterStore parameterStore, NDList inputs, boolean training) {
        NDList selfTransform = self.forward(parameterStore, new NDList(inputs.get(0)), training);
        NDArray res = bias == null? selfTransform.get(0).add(inputs.get(1)) : selfTransform.get(0).add(inputs.get(1)).add(bias.getArray());
        return new NDList(res);
    }


    @Override
    public Shape[] getOutputShapes(Shape[] inputShapes) {
        return new Shape[]{outputShape};
    }
}
