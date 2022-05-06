package functions.nn;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.pytorch.engine.PtModel;

import java.io.IOException;
import java.nio.file.Path;

public class StateDictLoader {

    public static Model loadModel() throws MalformedModelException, IOException {
        Model a = Model.newInstance("gnn");
        a.load(Path.of("/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/jupyter/models/GraphSage-Cora-2022-05-06"));
        return a;
    }
}
