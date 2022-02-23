from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction
from pyflink.datastream.connectors import FileSource, StreamFormat
from pyflink.common import WatermarkStrategy
from helpers.custom_key_by import key_by_custom
from helpers.model_version_watermark import MyTimestampAssigner, MyWaterMarkStrategy

from typing import TYPE_CHECKING
from elements import Op

if TYPE_CHECKING:
    from storage.gnn_layer import GNNLayerProcess
    from partitioner import BasePartitioner


class GraphStream:
    def __init__(self, PARALLELISM: int = 3, LAYERS=3):
        self.PARALLELISM = PARALLELISM
        self.LAYERS = LAYERS
        self.env: StreamExecutionEnvironment = StreamExecutionEnvironment.get_execution_environment()
        self.env.add_classpaths(
            "file:///home/rustambaku13/Documents/Warwick/flink-streaming-gnn/build/libs/Flink-Partitioningrg.bytedeco.openblas.platform {-1.0-SNAPSHOT.jar")

        # self.env.get_config().disable_closure_cleaner()
        self.env.set_parallelism(self.PARALLELISM)
        self.env.set_max_parallelism(self.PARALLELISM)
        self.env.get_config().set_auto_watermark_interval(60000)  # Retraining each 10000 seconds
        self.position_index = 1
        self.last: "DataStream" = None  # Last DataStream in this pipeline
        self.train_stream: "DataStream" = None
        self.iterator = None  # Outermost iterators that connects 2 gnn-process functions. Used for backwards pass

    def read_file(self, parser: "MapFunction", file_name: str = 'amazon0302_adj.tsv') -> DataStream:
        """ Read file as a line stream and parse using the @parser @todo not working for deployment?"""
        import pathlib, os
        cur_path = pathlib.Path().resolve()
        file_name = os.path.join(cur_path, "dataset", file_name)
        self.last = self.env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                       file_name).process_static_file_set().build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="File Reader").map(parser)
        return self.last

    def read_socket(self, parser: "MapFunction", host, port) -> DataStream:
        """ Read sochet for a streaming of lines """
        tmp = self.env._j_stream_execution_environment.socketTextStream(host, port)
        self.last = DataStream(tmp).name("Socket Reader")
        self.last = self.last.map(parser).name("String Parser")  # @todo fix later the ordering so that parallelism can
        return self.last

    def partition(self, partitioner: "BasePartitioner") -> DataStream:
        """ Partition incoming @GraphQuery data into parallel machines """
        partitioner_par = self.PARALLELISM
        partitioner.partitions = self.PARALLELISM
        if not partitioner.is_parallel(): partitioner_par = 1  # Partitioner does not support parallelism in itself
        self.last = self.last.map(partitioner).set_parallelism(partitioner_par).name("Partitioner")
        return self.last

    def gnn_layer(self, storageProcess: "GNNLayerProcess") -> DataStream:
        """ Add Storage engine as well as iteration with 2 filters. Iteration depends on @GraphQuery.iterate filed """
        storageProcess.layers = self.LAYERS
        storageProcess.position = self.position_index
        if self.position_index == 1:
            self.last = self.last.assign_timestamps_and_watermarks(MyWaterMarkStrategy.for_periodic_retraining())
        self.position_index += 1
        last = self.last
        iterator = last._j_data_stream.iterate().name("Self Iteration Source")  # Java Class need to somehow handle it
        ds = key_by_custom(DataStream(iterator))

        st = ds.process(storageProcess).name("GNN Process")
        iterate_filter = st.filter(lambda x: x.iterate is True).name("Self Iteration Filter")
        continue_filter = st.filter(lambda x: x.iterate is False).name("Next layer Filter")
        if self.iterator:
            back_filter = st.filter(lambda x: x.iterate is None).name("Backward Iteration Filter")
            self.iterator.closeWith(back_filter._j_data_stream)
        iterator.closeWith(iterate_filter._j_data_stream)
        self.last = continue_filter
        self.iterator = iterator
        return self.last

    def training_inference_layer(self, storageProcess: "GNNLayerProcess"):
        """ Different from gnn_layer in ways:
            1. No backward iteration afterwards, so self.long_iterator becomes zero
            2. Merging Training stream and previous data streams
         """
        storageProcess.layers = self.LAYERS
        storageProcess.position = self.position_index
        last = self.last.union(self.train_stream)

        # layer and training samples
        iterator = last._j_data_stream.iterate().name("Self Iteration Source")  # Java Class need to somehow handle it
        ds = DataStream(iterator)
        keyed_ds = key_by_custom(ds)
        st = keyed_ds.process(storageProcess).name("Training Process")
        iterate_filter = st.filter(lambda x: x.iterate is True).name("Self Iteration Filter")
        continue_filter = st.filter(lambda x: x.iterate is False).name("Next Layer Filter")
        if self.iterator:
            back_filter = st.filter(lambda x: x.iterate is None).name("Backward Iteration Filter")
            self.iterator.closeWith(back_filter._j_data_stream)

        iterator.closeWith(iterate_filter._j_data_stream)

        self.last = continue_filter
        self.iterator = None
        return self.last

    def train_test_split(self, splitter: "MapFunction"):
        splitter = self.last.map(splitter)
        self.last = splitter.filter(lambda x: not (x.op is Op.AGG and x.aggregator_name == '3trainer')).name(
            "Normal Data")
        self.train_stream = splitter.filter(lambda x: (x.op is Op.AGG and x.aggregator_name == '3trainer')).name(
            "Training Data")
