package ai.djl.nn.hgnn;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Parameter;
import ai.djl.nn.core.Linear;
import ai.djl.training.ParameterStore;

public final class HSageConv extends HGNNBlock {
    Linear neigh;

    Linear self;

    Parameter bias;

    Shape outputShape;

    public HSageConv(int outFeatures, boolean optBias) {
        if (optBias) {
            bias = Parameter.builder().setName("bias").setType(Parameter.Type.BIAS).build();
            addParameter(bias);
        }
        this.neigh = Linear.builder().setUnits(outFeatures).optBias(false).build();
        this.self = Linear.builder().setUnits(outFeatures).optBias(false).build();
        addChildBlock("self", self);
        addChildBlock("neigh", neigh);
        outputShape = new Shape(outFeatures);
    }

    @Override
    public void initialize(NDManager manager, DataType dataType, Shape... inputShapes) {
        beforeInitialize(inputShapes);
        this.neigh.initialize(manager, dataType, inputShapes);
        this.self.initialize(manager, dataType, inputShapes);
        if (bias != null && !bias.isInitialized()) {
            bias.setShape(outputShape);
            bias.initialize(manager, dataType);
        }
    }

    @Override
    public NDList message(ParameterStore parameterStore, NDList inputs, boolean training) {
        return neigh.forward(parameterStore, inputs, training);
    }

    @Override
    public NDList update(ParameterStore parameterStore, NDList inputs, boolean training) {
        NDList selfTransform = self.forward(parameterStore, new NDList(inputs.get(0)), training);
        NDArray res = bias == null ? selfTransform.get(0).add(inputs.get(1)) : selfTransform.get(0).add(inputs.get(1)).add(bias.getArray());
        return new NDList(res);
    }


    @Override
    public Shape[] getOutputShapes(Shape[] inputShapes) {
        return new Shape[]{outputShape};
    }
}
