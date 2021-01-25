from time import perf_counter


class ExecutionTime:
    """
    A context manager to measure
    execution time of a block.

    >>> with ExecutionTime() as timer:
    >>>     # do something
    >>>     pass
    >>> print(timer.elapsed)
    """
    def __init__(self):
        self.elapsed = None

    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type:
            return False
        self.stop = perf_counter()
        self.elapsed = self.stop - self.start
