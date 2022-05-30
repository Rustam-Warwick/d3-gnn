package operators.logger;

import elements.GraphOp;
import elements.Op;
import elements.iterations.MessageCommunication;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.server.ByteBufferOutputStream;
import org.apache.flink.statefun.flink.core.logger.UnboundedFeedbackLogger;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;

public class IterationQueueCheckpointLogger implements FeedbackLogger<StreamRecord<GraphOp>>{
    private final StateSnapshotContext context;
    private final TypeSerializer<StreamElement> serializer;
    private final KeySelector<StreamRecord<GraphOp>,?> keySelector;
    private final HashMap<Integer, KeyGroupStream<StreamRecord<GraphOp>>> keyGroupStreams;
    private KeyGroupStream<StreamRecord<GraphOp>> broadcastElementStream;
    private final int maxParallelism;
    private final IOManager ioManager;

    public IterationQueueCheckpointLogger(StateSnapshotContext context,
                                          TypeSerializer<StreamElement> serializer,
                                          KeySelector<StreamRecord<GraphOp>, ?> keySelector,
                                          int maxParallelism,
                                          IOManager ioManager
                                          ){
        this.context = context;
        this.serializer = serializer;
        this.keySelector = keySelector;
        this.keyGroupStreams = new HashMap<>();
        this.maxParallelism = maxParallelism;
        this.ioManager = ioManager;
        this.broadcastElementStream = null;

    }

    @Override
    public void append(StreamRecord<GraphOp> message) {
        try{
            if(message.getValue().getMessageCommunication() == MessageCommunication.BROADCAST && message.getValue().getOp()== Op.WATERMARK){
                // Pass for now, don't care about broadcast streams
                if(Objects.isNull(broadcastElementStream)) {
                    broadcastElementStream = new KeyGroupStream<>(serializer, ioManager, new MemorySegmentPool(MemorySize.ofMebiBytes(64).getBytes()));
                }
                broadcastElementStream.append(message);
            }
            else if(message.getValue().getMessageCommunication() == MessageCommunication.P2P){
                Object key = keySelector.getKey(message);
                int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
                keyGroupStreams.putIfAbsent(keyGroup, new KeyGroupStream<>(serializer,ioManager,new MemorySegmentPool(MemorySize.ofMebiBytes(64).getBytes())));
                keyGroupStreams.get(keyGroup).append(message);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void commit() {
        try {
            flushToKeyedStateOutputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            keyGroupStreams.clear();
        }
    }

    private void flushToKeyedStateOutputStream() throws IOException, Exception {
        final DataOutputView target = new DataOutputViewStreamWrapper(context.getRawKeyedOperatorStateOutput());
        final Iterable<Integer> assignedKeyGroupIds = keyGroupStreams.keySet();
        for (Integer keyGroupId : assignedKeyGroupIds) {
            context.getRawKeyedOperatorStateOutput().startNewKeyGroup(keyGroupId);
            keyGroupStreams.get(keyGroupId).writeTo(target);
        }
    }


}
