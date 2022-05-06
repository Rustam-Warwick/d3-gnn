package functions.nn;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.nn.SequentialBlock;
import functions.nn.gnn.GNNBlock;
import functions.nn.gnn.SAGEConv;

import java.io.IOException;

public class StateDictLoader {
    public static Model loadModel() throws MalformedModelException, IOException {
        GNNBlock a = new SAGEConv(128);
        a.initialize();



        return null;
    }
}
