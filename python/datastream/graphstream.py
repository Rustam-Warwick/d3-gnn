from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction
from pyflink.datastream.data_stream import _get_one_input_stream_operator
from pyflink.datastream.connectors import FileSource, StreamFormat, Duration
from pyflink.common import WatermarkStrategy
from pyflink.fn_execution import flink_fn_execution_pb2
from partitioner import Partitioner, KeySelector
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage.process_fn import GraphStorageProcess
    from partitioner import BasePartitioner


class GraphStream:
    def __init__(self, PARALLELISM: int = 3):
        self.PARALLELISM = PARALLELISM
        self.env: StreamExecutionEnvironment = StreamExecutionEnvironment.get_execution_environment()
        # self.env.get_config().disable_closure_cleaner()
        self.env.set_parallelism(self.PARALLELISM)
        self.last: "DataStream" = None  # Last DataStream in this pipeline
        self.long_iterator = None
        self._j_data_stream = None

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
        tmp = self.env._j_stream_execution_environment.socketTextStream(host, port)  # Using Java BC Python API does
        # not support this
        self.last = DataStream(tmp)
        self.last = self.last.map(parser)
        return self.last

    def partition(self, partitioner: "BasePartitioner") -> DataStream:
        """ Partition incoming @GraphQuery data into parallel machines """
        partitioner_par = self.PARALLELISM
        partitioner.partitions = self.PARALLELISM
        if not partitioner.is_parallel(): partitioner_par = 1 # Partitioner does not support parallelism
        self.last = self.last.map(partitioner).set_parallelism(partitioner_par)
        return self.last

    def storage(self, storageProcess: "GraphStorageProcess") -> DataStream:
        """ Add Storage engine as well as iteration with 2 filters. Iteration depends on @GraphQuery.iterate fild """
        last = self.last.partition_custom(Partitioner(), KeySelector())
        iterator = last._j_data_stream.iterate()  # Java Class need to somehow handle it
        ds = DataStream(iterator)
        long_iterator = None
        if not storageProcess.is_last:
            long_iterator = last._j_data_stream.iterate()  # Java Class need to somehow handle it
            ds = ds.union(DataStream(long_iterator))
        st = ds.process(storageProcess)
        iterate_filter = st.filter(lambda x: x.iterate is True).partition_custom(Partitioner(), KeySelector())
        continue_filter = st.filter(lambda x: x.iterate is False)
        if self.long_iterator:
            back_filter = st.filter(lambda x: x.iterate is None).partition_custom(Partitioner(), KeySelector())  # Back
            self.long_iterator.closeWith(back_filter._j_data_stream)
        iterator.closeWith(iterate_filter._j_data_stream)
        self.last = continue_filter
        self.long_iterator = long_iterator
        return self.last
