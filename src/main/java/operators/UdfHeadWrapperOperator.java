package operators;

import elements.GraphOp;
import operators.coordinators.events.StartTraining;
import operators.coordinators.events.StopTraining;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheReader;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheSnapshot;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheWriter;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Head Operator that receives all external inputs to the graph. Handles buffering while training and splitting messages
 *
 * @param <T> Internal operator
 */
public class UdfHeadWrapperOperator<T extends AbstractUdfStreamOperator<GraphOp, ? extends Function> & OneInputStreamOperator<GraphOp, GraphOp>> extends BaseWrapperOperator<T> implements OneInputStreamOperator<GraphOp, GraphOp> {

    private Path basePath;

    private FileSystem fileSystem;

    private TypeSerializer<StreamElement> typeSerializer;

    private DataCacheWriter<StreamElement> dataCacheWriter;

    @Nullable
    private DataCacheReader<StreamElement> currentDataCacheReader;


    public UdfHeadWrapperOperator(StreamOperatorParameters<GraphOp> parameters, StreamOperatorFactory<GraphOp> operatorFactory, IterationID iterationID, short position, short totalLayers) {
        super(parameters, operatorFactory, iterationID, position, totalLayers, (short) 0);
        this.ITERATION_COUNT = 0; // No watermark iteration at all needed for this operator
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
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        try {
            SupplierWithException<Path, IOException> pathGenerator =
                    OperatorUtils.createDataCacheFileGenerator(
                            basePath, "buffer", getOperatorID());

            DataCacheSnapshot dataCacheSnapshot = null;
            List<StatePartitionStreamProvider> rawStateInputs =
                    IteratorUtils.toList(context.getRawOperatorStateInputs().iterator());
            if (rawStateInputs.size() > 0) {
                checkState(
                        rawStateInputs.size() == 1,
                        "Currently the replay operator does not support rescaling");

                dataCacheSnapshot =
                        DataCacheSnapshot.recover(
                                rawStateInputs.get(0).getStream(), fileSystem, pathGenerator);
            }

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

    @Override
    public void processActualElement(StreamRecord<GraphOp> element) throws Exception {
        if (WATERMARK_STATUSES.f3 == WatermarkStatus.IDLE) {
            dataCacheWriter.addRecord(element);
        } else {
            getWrappedOperator().processElement(element);
        }
    }

    @Override
    public void processActualWatermark(Watermark mark) throws Exception {
        getWrappedOperator().processWatermark(mark);
    }

    @Override
    public void processActualWatermarkStatus(WatermarkStatus status) throws Exception {
        getWrappedOperator().processWatermarkStatus(status);
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        if (evt instanceof StartTraining) {
            try {
                processWatermarkStatus(WatermarkStatus.IDLE); // Mark the subsequent watermarks as idle
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (evt instanceof StopTraining) {
            try {
                processWatermarkStatus(WatermarkStatus.ACTIVE); // Mark the watermark status as active
                dataCacheWriter.finish();
                checkState(currentDataCacheReader == null, "Concurrent replay is not supported");
                currentDataCacheReader =
                        new DataCacheReader<>(
                                typeSerializer, fileSystem, dataCacheWriter.getFinishSegments());
                replayRecords(currentDataCacheReader);
                acknowledgeIfWatermarkIsReady();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void replayRecords(DataCacheReader<StreamElement> dataCacheReader) throws Exception {
        while (dataCacheReader.hasNext()) {
            // we first process the pending mail
            StreamRecord<GraphOp> next = (StreamRecord<GraphOp>) dataCacheReader.next();
            setKeyContextElement(next);
            processElement(next);
        }
        currentDataCacheReader = null;
    }

    @Override
    public void setKeyContextElement(StreamRecord<GraphOp> record) throws Exception {
        super.setKeyContextElement(record);
        getWrappedOperator().setKeyContextElement(record);
    }

}
