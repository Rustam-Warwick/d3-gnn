package helpers;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.python.KeyByKeySelector;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Constructor;
import java.util.Objects;


public class MyKeyGroupStreamPartitioner extends StreamPartitioner<Row>
        implements ConfigurableStreamPartitioner {
    private static final long serialVersionUID = 1L;

    private final KeySelector<Row, Row> keySelector;

    private int maxParallelism;

    public MyKeyGroupStreamPartitioner() {

        this.keySelector = new KeyByKeySelector();
        this.maxParallelism = StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
        Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<Row>> record) {
        Row key;
        try {
            key = keySelector.getKey(record.getInstance().getValue());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not extract key from " + record.getInstance().getValue(), e);
        }
        return (int) key.getField(0);
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.ARBITRARY;
    }

    @Override
    public StreamPartitioner<Row> copy() {
        return this;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "HASH";
    }

    @Override
    public void configure(int maxParallelism) {
        KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
        this.maxParallelism = maxParallelism;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final  MyKeyGroupStreamPartitioner that = (MyKeyGroupStreamPartitioner) o;
        return maxParallelism == that.maxParallelism && keySelector.equals(that.keySelector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keySelector, maxParallelism);
    }

    public KeyedStream<Row, Integer> keyBy(DataStream<Row> inputStream) throws Exception{
        Constructor<KeyedStream> constructor = KeyedStream.class.getDeclaredConstructor(DataStream.class, PartitionTransformation.class, KeySelector.class, TypeInformation.class);
        constructor.setAccessible(true);
        return constructor.newInstance(
                inputStream,
                new PartitionTransformation<>(
                        inputStream.getTransformation(),
                        this),
                this.keySelector,
                Types.ROW(Types.INT)
        );
    }
}
