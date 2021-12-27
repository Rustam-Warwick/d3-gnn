from elements import Rpc


def wrap_to_rpc(fn: object) -> callable:
    """ Wraps class instance methods to Rpc object
        Used to make send the RPC call over network in case the called element is replicated
    """
    def wrapper(self, *args, **kwargs):
        rpc = Rpc(fn, self, *args, **kwargs)
        self(rpc)

    return wrapper
