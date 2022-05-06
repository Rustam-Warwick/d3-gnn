package functions.nn.gnn;

import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.AbstractBlock;
import ai.djl.nn.Block;
import ai.djl.nn.core.Linear;
import ai.djl.training.ParameterStore;
import ai.djl.util.PairList;
import scala.xml.PrettyPrinter;

abstract public class GNNBlock extends AbstractBlock {
    @Override
    protected NDList forwardInternal(ParameterStore parameterStore, NDList inputs, boolean training, PairList<String, Object> params) {
        throw new IllegalStateException("GNNBlock are not used separately");
    }

    public abstract NDList message(ParameterStore parameterStore, NDList inputs, boolean training);
    public abstract NDList update(ParameterStore parameterStore, NDList inputs, boolean training);
}
