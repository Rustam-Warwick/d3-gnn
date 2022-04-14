package helpers;

import org.apache.flink.api.common.eventtime.*;


public class PeriodicTrainingWatermarkStrategy<T> implements WatermarkStrategy<T> {
    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new PeriodicTrainingWatermarkGenerator<>();
    }

    @Override
    public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return WatermarkStrategy.super.createTimestampAssigner(context);
    }

    public class PeriodicTrainingWatermarkGenerator<T> implements WatermarkGenerator<T> {
        public int modelVersion = 0;

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(++modelVersion));
        }
    }

}
