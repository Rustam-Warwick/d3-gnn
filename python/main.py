from pyflink.datastream import StreamExecutionEnvironment
from helpers.socketmapper import *
from storage.map_storage import HashMapStorage
PARALLELISM = 3


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.get_config().disable_closure_cleaner()
    ds = file_reader(env)
    ds = ds.map(EdgeListParser())
    ds = ds.process(HashMapStorage())
    ds.print()
    env.execute("Test Python job")


if __name__ == '__main__':
    run()
