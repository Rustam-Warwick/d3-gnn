from elements import Rpc


def wrap_to_rpc(fn):
    def wrapper(self, *args, **kwargs):
        rpc = Rpc(fn, self, *args, **kwargs)
        self(rpc)

    return wrapper
