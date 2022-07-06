package operators.iterations;

import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.CountingBroadcastOutput;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.List;

public class IndividualBroadcastOutput<OUT> implements BroadcastOutput<OUT> {
    protected final Counter numRecordsOut;
    protected final List<BroadcastOutput<OUT>> internalOutputs;

    public IndividualBroadcastOutput(
            Counter numRecordsOut, List<BroadcastOutput<OUT>> internalOutputs) {
        this.numRecordsOut = numRecordsOut;
        this.internalOutputs = internalOutputs;
    }

    @Override
    public void broadcastEmit(StreamRecord<OUT> record) throws IOException {
        numRecordsOut.inc();

        for (BroadcastOutput<OUT> internalOutput : internalOutputs) {
            internalOutput.broadcastEmit(record);
        }
    }

    @Override
    public void broadcastEmit(StreamRecord<OUT> record, OutputTag<OUT>) throws IOException {
        numRecordsOut.inc();
        for (BroadcastOutput<OUT> internalOutput : internalOutputs) {
            internalOutput.broadcastEmit(record);
        }
    }

}
