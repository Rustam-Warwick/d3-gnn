package functions.nn;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.nn.BlockList;
import ai.djl.nn.SequentialBlock;
import functions.nn.gnn.AggregatorVariant;
import functions.nn.gnn.GNNBlock;
import functions.nn.gnn.SAGEConv;
import scala.collection.Seq;

import java.io.IOException;

public class StateDictLoader {
    public static BlockList loadModel() throws MalformedModelException, IOException {
        SequentialBlock m = new SequentialBlock();
        m.add(new SAGEConv(128));
        m.add(new SAGEConv(128));
        return m.getChildren();
    }
}
