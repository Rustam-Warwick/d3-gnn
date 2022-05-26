package helpers;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.*;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;

public class ExternalOperatorSnapshotFutures extends OperatorSnapshotFutures {
    protected final long checkpointId;
    protected final long timestamp;
    protected final CheckpointOptions checkpointOptions;
    protected final CheckpointStreamFactory storageLocation;

    public ExternalOperatorSnapshotFutures(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, CheckpointStreamFactory storageLocation) {
        super(
            new PendingWrapperFuture<SnapshotResult<KeyedStateHandle>>(),
            new PendingWrapperFuture<SnapshotResult<KeyedStateHandle>>(),
            new PendingWrapperFuture<SnapshotResult<OperatorStateHandle>>(),
            new PendingWrapperFuture<SnapshotResult<OperatorStateHandle>>(),
            new PendingWrapperFuture<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>(),
            new PendingWrapperFuture<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>()
        );
        this.checkpointId = checkpointId;
        this.timestamp = timestamp;
        this.checkpointOptions = checkpointOptions;
        this.storageLocation = storageLocation;
    }
}
