package operators;

import elements.GraphOp;
import elements.iterations.MessageCommunication;
import operators.coordinators.events.StartTraining;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheReader;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheSnapshot;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheWriter;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;

/**
 * Head Operator that receives all external inputs to the graph. Handles buffering while training and splitting messages
 * @param <T> Internal operator
 */
public class ChainHeadUdfOperator<T extends AbstractUdfStreamOperator<GraphOp, ? extends Function> & OneInputStreamOperator<GraphOp, GraphOp>> extends BaseWrapperOperator<T> implements OneInputStreamOperator<GraphOp, GraphOp> {

    private final MailboxExecutor bufferMailboxExecutor;

    private Path basePath;

    private FileSystem fileSystem;


    private TypeSerializer<StreamElement> typeSerializer;


    private DataCacheWriter<StreamElement> dataCacheWriter;


    @Nullable
    private DataCacheReader<StreamElement> currentDataCacheReader;

    public ChainHeadUdfOperator(StreamOperatorParameters<GraphOp> parameters, StreamOperatorFactory<GraphOp> operatorFactory, IterationID iterationID, short position, short totalLayers) {
        super(parameters, operatorFactory, iterationID, position, totalLayers);
        this.ITERATION_COUNT = 0; // No watermark iteration at all needed for this operator
        this.bufferMailboxExecutor = parameters.getContainingTask().getMailboxExecutorFactory().createExecutor(TaskMailbox.MIN_PRIORITY);
        try {
            basePath =
                    OperatorUtils.getDataCachePath(
                            containingTask.getEnvironment().getTaskManagerInfo().getConfiguration(),
                            containingTask
                                    .getEnvironment()
                                    .getIOManager()
                                    .getSpillingDirectoriesPaths());

            fileSystem = basePath.getFileSystem();
            typeSerializer =
                            new StreamElementSerializer<>(parameters.getStreamConfig().getTypeSerializerOut(getClass().getClassLoader()));
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {
        super.initializeState(streamTaskStateManager);
        try {
            SupplierWithException<Path, IOException> pathGenerator =
                    OperatorUtils.createDataCacheFileGenerator(
                            basePath, "buffer", getOperatorID());

            DataCacheSnapshot dataCacheSnapshot = null;
//            final CloseableRegistry streamTaskCloseableRegistry =
//                    Preconditions.checkNotNull(containingTask.getCancelables());
//            final StreamOperatorStateContext context =
//                    streamTaskStateManager.streamOperatorStateContext(
//                            getOperatorID(),
//                            getClass().getSimpleName(),
//                            wrappedOperator.getProcessingTimeService(),
//                            this,
//                            wrappedOperator.getKeyedStateBackend().getKeySerializer(),
//                            streamTaskCloseableRegistry,
//                            metrics,
//                            parameters.getStreamConfig().getManagedMemoryFractionOperatorUseCaseOfSlot(
//                                    ManagedMemoryUseCase.STATE_BACKEND,
//                                    wrappedOperator.getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration(),
//                                    wrappedOperator.getRuntimeContext().getUserCodeClassLoader()),
//                            false);
//
//            List<StatePartitionStreamProvider> rawStateInputs =
//                    wrappedOperator.
//                    IteratorUtils.toList();
//            if (rawStateInputs.size() > 0) {
//                checkState(
//                        rawStateInputs.size() == 1,
//                        "Currently the replay operator does not support rescaling");
//
//                dataCacheSnapshot =
//                        DataCacheSnapshot.recover(
//                                rawStateInputs.get(0).getStream(), fileSystem, pathGenerator);
//            }

            dataCacheWriter =
                    new DataCacheWriter<>(
                            typeSerializer,
                            fileSystem,
                            pathGenerator,
                            dataCacheSnapshot == null
                                    ? Collections.emptyList()
                                    : dataCacheSnapshot.getSegments());

            if (dataCacheSnapshot != null && dataCacheSnapshot.getReaderPosition() != null) {
                currentDataCacheReader =
                        new DataCacheReader<>(
                                typeSerializer,
                                fileSystem,
                                dataCacheSnapshot.getSegments(),
                                dataCacheSnapshot.getReaderPosition());
            }

        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to replay the records", e);
        }
    }

    /**
     * Broadcast elements should go to all parts
     */
    @Override
    public void processActualElement(StreamRecord<GraphOp> element) throws Exception {
        if(IS_TRAINING){
            dataCacheWriter.addRecord(element);
        } else {
            if (element.getValue().getMessageCommunication() == MessageCommunication.BROADCAST) {
                // Broadcast messages invoked in all the parts
                for (short part : thisParts) {
                    element.getValue().setPartId(part);
                    setKeyContextElement(element);
                    getWrappedOperator().processElement(element);
                }
            } else {
                getWrappedOperator().processElement(element);
            }
        }
    }

    @Override
    public void processActualWatermark(Watermark mark) throws Exception {
        getWrappedOperator().processWatermark(mark);
    }


    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        if(evt instanceof StartTraining){
            IS_TRAINING = true;
            try {
                processWatermarkStatus(WatermarkStatus.IDLE);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void setKeyContextElement(StreamRecord<GraphOp> record) throws Exception {
        super.setKeyContextElement(record);
        getWrappedOperator().setKeyContextElement(record);
    }

}
