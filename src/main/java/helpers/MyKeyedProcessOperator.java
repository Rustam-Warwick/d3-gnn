package helpers;

import elements.GraphOp;
import functions.GraphProcessFn;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OnWatermarkCallback;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import storage.BaseStorage;

public class MyKeyedProcessOperator extends KeyedProcessOperator<String,GraphOp, GraphOp> {
    public MyKeyedProcessOperator(BaseStorage function) {
        super(function);
    }
}
