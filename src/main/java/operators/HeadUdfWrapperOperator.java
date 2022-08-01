package operators;

import elements.GraphOp;
import elements.Op;
import operators.events.StartTraining;
import operators.events.StopTraining;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheReader;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheWriter;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;

/**
 * Wrapper around any other operator
 * Acts as the <strong>Splitter</strong> operator of the paper
 *
 * @param <T> Internal operator
 */
public class HeadUdfWrapperOperator<T extends AbstractUdfStreamOperator<GraphOp, ? extends Function> & OneInputStreamOperator<GraphOp, GraphOp>> extends BaseWrapperOperator<T> implements OneInputStreamOperator<GraphOp, GraphOp> {

    private Path basePath;

    private FileSystem fileSystem;

    private TypeSerializer<StreamElement> typeSerializer;

    private DataCacheWriter<StreamElement> dataCacheWriter;

    private boolean TRAINING = false;

    @Nullable
    private DataCacheReader<StreamElement> currentDataCacheReader;


    public HeadUdfWrapperOperator(StreamOperatorParameters<GraphOp> parameters, StreamOperatorFactory<GraphOp> operatorFactory, IterationID iterationID, short position, short totalLayers) {
        super(parameters, operatorFactory, iterationID, position, totalLayers);
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

            dataCacheWriter =
                    new DataCacheWriter<>(
                            typeSerializer,
                            fileSystem,
                            pathGenerator,
                            Collections.emptyList());

        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to replay the records", e);
        }
    }

    @Override
    public void processActualElement(StreamRecord<GraphOp> element) throws Exception {
        if (element.getValue().getOp() == Op.OPERATOR_EVENT) {
            if (element.getValue().getOperatorEvent() instanceof StartTraining) {
                TRAINING = true;
                ((StartTraining) element.getValue().getOperatorEvent()).setBroadcastCount(parallelism);
//                context.broadcastOutput(new GraphOp(element.getValue().getOperatorEvent()));
            } else if (element.getValue().getOperatorEvent() instanceof StopTraining) {
                try {
                    TRAINING = false;
                    dataCacheWriter.finishCurrentSegment();
                    currentDataCacheReader =
                            new DataCacheReader<>(
                                    typeSerializer, fileSystem, dataCacheWriter.getFinishSegments());
                    replayRecords(currentDataCacheReader);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            element.setTimestamp(getWrappedOperator().getProcessingTimeService().getCurrentProcessingTime());
            if (TRAINING) {
                dataCacheWriter.addRecord(element);
            } else {
                getWrappedOperator().processElement(element);
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
