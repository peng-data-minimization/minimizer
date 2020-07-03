import functools
from collections.abc import Iterable


class WrongInputDataTypeException(Exception):
    pass


def check_input_type(func):
    # assumes that data is in first position of args
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        data = args[0]

        if not data:
            return func(*args, **kwargs)

        if not isinstance(data, Iterable):
            raise WrongInputDataTypeException("Input data must be of type Iterable.")

        # check only first element of list
        if not isinstance(next(iter(data)), dict):
            raise WrongInputDataTypeException("Data elements must be of type dict.")

        return func(*args, **kwargs)

    return wrapper
