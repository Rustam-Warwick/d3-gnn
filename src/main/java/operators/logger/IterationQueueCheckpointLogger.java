package operators.logger;

import elements.GraphOp;
import elements.Op;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;
import java.io.IOException;

public class IterationQueueCheckpointLogger implements FeedbackLogger<StreamRecord<GraphOp>> {
    private final StateSnapshotContext context;
    private final KeyGroupStream<StreamRecord<GraphOp>> operatorStream;

    public IterationQueueCheckpointLogger(@Nullable StateSnapshotContext context, @Nullable StateInitializationContext initContext,
                                          TypeSerializer<StreamElement> serializer,
                                          IOManager ioManager
    ) {
        this.context = context;
        this.operatorStream = new KeyGroupStream<>(serializer, ioManager, new MemorySegmentPool(MemorySize.ofMebiBytes(64).getBytes()));
    }

    @Override
    public void append(StreamRecord<GraphOp> message) {
        try {
            System.out.println("Logging something");
            if (message.getValue().getOp() == Op.WATERMARK)
                return; // Watermarks messages should not be checkpointed since they are re-run on startup
            operatorStream.append(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {

    }

    public void restoreState() {

    }

    @Override
    public void commit() {
        try {
            operatorStream.writeTo(new DataOutputViewStreamWrapper(context.getRawOperatorStateOutput()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
