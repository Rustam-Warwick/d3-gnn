package ai.djl.nn.gnn;

import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.core.Linear;
import ai.djl.training.ParameterStore;

public class GATConv extends GNNBlock{

    Linear self;

    Linear att;

    Shape outputShape;

    public GATConv(int outFeatures){
        this.self = Linear.builder().setUnits(outFeatures).optBias(false).build();
        this.att = Linear.builder().setUnits(1).optBias(false).build();
        this.outputShape = new Shape(outFeatures);
    }

    @Override
    public NDList message(ParameterStore parameterStore, NDList inputs, boolean training) {
        return null;
    }

    @Override
    public NDList update(ParameterStore parameterStore, NDList inputs, boolean training) {
        return null;
    }

    @Override
    public Shape[] getOutputShapes(Shape[] inputShapes) {
        return new Shape[]{outputShape};
    }
}
