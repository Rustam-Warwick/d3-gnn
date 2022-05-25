package operators.coordinators;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A specific coordinator event handler
 */
public interface WrapperOperatorEventHandler extends OperatorCoordinator {
    WrapperOperatorCoordinator getCoordinator();

    void setCoordinator(WrapperOperatorCoordinator coordinator);

    List<Class<? extends OperatorEvent>> getEventClasses();

    @Override
    default void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {

    }

    @Override
    default void notifyCheckpointComplete(long checkpointId) {

    }

    @Override
    default void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {

    }

}
