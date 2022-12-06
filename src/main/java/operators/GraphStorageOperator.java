package operators;

import elements.GraphOp;
import elements.Plugin;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import storage.BaseStorage;

import java.util.List;

/**
 * Graph Storage operator that contains {@link Plugin} and {@link BaseStorage}
 */
public class GraphStorageOperator extends AbstractStreamOperator<GraphOp> implements OneInputStreamOperator<GraphOp, GraphOp> {

    private final List<Plugin> plugins;

    private final BaseStorage graphStorage;

    public GraphStorageOperator(List<Plugin> plugins, BaseStorage graphStorage) {
        this.plugins = plugins;
        this.graphStorage = graphStorage;
    }

    @Override
    public void processElement(StreamRecord<GraphOp> element) throws Exception {
        System.out.println("Graph Storage");
    }

}
