from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction
from pyflink.datastream.connectors import FileSource, StreamFormat
from pyflink.common import WatermarkStrategy
from partitioner import Partitioner, KeySelector,GraphElementIdSelector
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage.gnn_layer import GNNLayerProcess
    from partitioner import BasePartitioner


class GraphStream:
    def __init__(self, PARALLELISM: int = 3):
        self.PARALLELISM = PARALLELISM
        self.env: StreamExecutionEnvironment = StreamExecutionEnvironment.get_execution_environment()
        # self.env.get_config().disable_closure_cleaner()
        self.env.set_parallelism(self.PARALLELISM)
        self.env.set_max_parallelism(self.PARALLELISM)
        self.last: "DataStream" = None  # Last DataStream in this pipeline
        self.train_stream: "DataStream" = None
        self.long_iterator = None  # Outermost iterators that connects 2 gnn-process functions. Used for backwards pass

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
        self.last = self.last.map(parser).set_parallelism(1).name("String Parser")  # @todo fix later the ordering so that parallelism can
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
        last = self.last.partition_custom(Partitioner(), KeySelector())
        long_iterator = last._j_data_stream.iterate()  # Java Class need to somehow handle it
        iterator = long_iterator.iterate()  # Java Class need to somehow handle it

        ds = DataStream(iterator)
        st = ds.process(storageProcess).name("GNN Process")
        iterate_filter = st.filter(lambda x: x.iterate is True).partition_custom(Partitioner(), KeySelector())
        continue_filter = st.filter(lambda x: x.iterate is False)
        if self.long_iterator:
            back_filter = st.filter(lambda x: x.iterate is None).partition_custom(Partitioner(), KeySelector())  # Back
            self.long_iterator.closeWith(back_filter._j_data_stream)
        iterator.closeWith(iterate_filter._j_data_stream)
        self.last = continue_filter
        self.long_iterator = long_iterator
        return self.last

    def training_inference_layer(self, storageProcess: "GNNLayerProcess"):
        """ Different from gnn_layer in ways:
            1. No backward iteration afterwards, so self.long_iterator becomes zero
            2. Merging Training stream and previous data streams
         """
        storageProcess.is_last = True
        last = self.last.union(self.train_stream).partition_custom(Partitioner(), KeySelector())

        # layer and training samples
        iterator = last._j_data_stream.iterate()  # Java Class need to somehow handle it
        ds = DataStream(iterator)
        st = ds.process(storageProcess)
        iterate_filter = st.filter(lambda x: x.iterate is True).partition_custom(Partitioner(), KeySelector())
        continue_filter = st.filter(lambda x: x.iterate is False)
        if self.long_iterator:
            back_filter = st.filter(lambda x: x.iterate is None).partition_custom(Partitioner(), KeySelector())  # Back
            self.long_iterator.closeWith(back_filter._j_data_stream)

        iterator.closeWith(iterate_filter._j_data_stream)

        self.last = continue_filter
        self.long_iterator = None
        return self.last

    def train_test_split(self, splitter: "MapFunction"):
        splitter = self.last.map(splitter)
        self.last = splitter.filter(lambda x: x.is_train is False)
        self.train_stream = splitter.filter(lambda x: x.is_train is True)
