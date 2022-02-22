from typing import Any
from pyflink.java_gateway import get_gateway, java_import
from pyflink.common.watermark_strategy import WatermarkStrategy, Duration, TimestampAssigner


class MyTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return max(record_timestamp + 1, 0)


class MyWaterMarkStrategy(WatermarkStrategy):
    @staticmethod
    def for_periodic_retraining():
        gateway = get_gateway()
        java_import(gateway.jvm, "org.apache.flink.rustam.gnn.helpers.PeriodicTrainingWatermarkStrategy")
        JWaterMarkStrategy = get_gateway().jvm.helpers.PeriodicTrainingWatermarkStrategy
        return MyWaterMarkStrategy(JWaterMarkStrategy())

