package typeinfo;

import elements.GraphElement;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class GraphElementTypeSerializer extends TypeSerializer<GraphElement> {
    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<GraphElement> duplicate() {
        return null;
    }

    @Override
    public GraphElement createInstance() {
        return null;
    }

    @Override
    public GraphElement copy(GraphElement from) {
        return null;
    }

    @Override
    public GraphElement copy(GraphElement from, GraphElement reuse) {
        return null;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void serialize(GraphElement record, DataOutputView target) throws IOException {

    }

    @Override
    public GraphElement deserialize(DataInputView source) throws IOException {
        return null;
    }

    @Override
    public GraphElement deserialize(GraphElement reuse, DataInputView source) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {

    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<GraphElement> snapshotConfiguration() {
        return null;
    }
}
