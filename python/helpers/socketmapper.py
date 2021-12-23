from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction
from pyflink.datastream.connectors import FileSource, StreamFormat
from pyflink.common import WatermarkStrategy
from elements.vertex import SimpleVertex
from elements.edge import SimpleEdge
from elements import GraphQuery, Op


class EdgeListParser(MapFunction):
    def map(self, value: str) -> GraphQuery:
        values = value.split("\t")
        a = SimpleVertex(master_part=-1, element_id=values[0])
        b = SimpleVertex(master_part=-1, element_id=values[0])
        edge = SimpleEdge(src=a, dest=b)
        query = GraphQuery(Op.ADD, edge)
        return query


def file_reader(env: StreamExecutionEnvironment, file_name: str = 'amazon0302_adj.tsv') -> DataStream:
    return env.from_source(
        source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                   file_name).process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="File Reader")
