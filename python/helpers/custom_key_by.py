from pyflink.java_gateway import get_gateway
from pyflink.datastream import DataStream, RuntimeContext
from pyflink.datastream import ProcessFunction, KeySelector, KeyedStream
from py4j.java_gateway import java_import, get_java_class, get_field, get_method
from pyflink.common import Row, typeinfo
from partitioner.base_partitioner import MyKeySelector
from pyflink.common.typeinfo import Types

__imported_dependencies: bool = False


def key_by_custom(input: DataStream):
    global __imported_dependencies
    gateway = get_gateway()
    if not __imported_dependencies:
        java_import(gateway.jvm, "org.apache.flink.rustam.gnn.helpers.MyKeyGroupStreamPartitioner")
        __imported_dependencies = True

    class AddKey(ProcessFunction):

        def __init__(self, key_selector):
            if isinstance(key_selector, MyKeySelector):
                self._key_selector_open_func = key_selector.open
                self._key_selector_close_func = key_selector.close
                self._get_key_func = key_selector.get_key
            else:
                self._key_selector_open_func = None
                self._key_selector_close_func = None
                self._get_key_func = key_selector

        def open(self, runtime_context: RuntimeContext):
            if self._key_selector_open_func:
                self._key_selector_open_func(runtime_context)

        def close(self):
            if self._key_selector_close_func:
                self._key_selector_close_func()

        def process_element(self, value, ctx: 'ProcessFunction.Context'):
            yield Row(self._get_key_func(value), value)

    output_type_info = typeinfo._from_java_type(
        input._j_data_stream.getTransformation().getOutputType())  # GraphElement
    key_type = Types.INT()
    stream_with_key_info = input.process(
        AddKey(MyKeySelector()),
        output_type=Types.ROW([key_type, output_type_info]))
    stream_with_key_info.name(gateway.jvm.org.apache.flink.python.util.PythonConfigUtil
                              .STREAM_KEY_BY_MAP_OPERATOR_NAME)

    my_custom_partitioner = gateway.jvm.helpers.MyKeyGroupStreamPartitioner()
    keyed_stream = my_custom_partitioner.keyBy(stream_with_key_info._j_data_stream)
    return KeyedStream(keyed_stream, output_type_info, input)
