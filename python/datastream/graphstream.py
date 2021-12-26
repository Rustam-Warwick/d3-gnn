from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction
from pyflink.datastream.data_stream import _get_one_input_stream_operator
from pyflink.datastream.connectors import FileSource, StreamFormat
from pyflink.common import WatermarkStrategy
from partitioner import Partitioner, KeySelector
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage import BaseStorage
    from partitioner import BasePartitioner


class GraphStream:
    def __init__(self, PARALLELISM: int = 3):
        self.PARALLELISM = PARALLELISM
        self.env: StreamExecutionEnvironment = StreamExecutionEnvironment.get_execution_environment()
        self.env.get_config().disable_closure_cleaner()
        self.env.set_parallelism(self.PARALLELISM)
        self.last: "DataStream" = None
        self._j_data_stream = None

    def read_file(self, parser: "MapFunction", file_name: str = 'amazon0302_adj.tsv') -> DataStream:
        """ Read file as a line stream and parse using the @parser """
        self.last = self.env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                       file_name).process_static_file_set().build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="File Reader").map(parser)
        return self.last

    def partition(self, partitioner: "BasePartitioner") -> DataStream:
        """ Partition incoming @GraphQuery data into parallel machines """
        self.last = self.last.map(partitioner).partition_custom(Partitioner(), KeySelector())
        return self.last

    def storage(self, storageProcess: "BaseStorage") -> DataStream:
        """ Add Storage engine as well as iteration with 2 filters. Iteration depends on @GraphQuery.iterate fild """
        from pyflink.fn_execution import flink_fn_execution_pb2
        iterator = self.last._j_data_stream.iterate()  # Java Class need to somehow handle it
        self._j_data_stream = iterator
        j_python_data_stream_function_operator, j_output_type_info = \
            _get_one_input_stream_operator(
                self,
                storageProcess,
                flink_fn_execution_pb2.UserDefinedDataStreamFunction.PROCESS,  # type: ignore
                None)

        st = DataStream(iterator.transform(
            "PROCESS",
            j_output_type_info,
            j_python_data_stream_function_operator))

        iterate_filter = st.filter(lambda x: x.iterate)
        continue_filter = st.filter(lambda x: not x.iterate)
        iterator.closeWith(iterate_filter._j_data_stream)
        self.last = continue_filter
        return self.last
