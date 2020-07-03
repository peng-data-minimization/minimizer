import hashlib
import statistics
from collections import Iterable
from typing import Callable
from numpy.random import default_rng
from .utils import check_input_type
from functools import partial


@check_input_type
def drop_keys(data: [dict], keys):
    return _replace_with_function(data, keys, _reset_value)


@check_input_type
def hash_keys(data: [dict], keys, hash_algorithm=hashlib.sha256, salt=None, digest_to_bytes=False):
    return _replace_with_function(data, keys, _hashing_wrapper, hash_algorithm=hash_algorithm,
                                  digest_to_bytes=digest_to_bytes, salt=salt)


@check_input_type
def replace_with_distribution(data: [dict], keys, numpy_distribution_function_str='standard_normal', *distribution_args,
                              **distribution_kwargs):
    # for possible distribution functions see
    # https://numpy.org/doc/stable/reference/random/generator.html#numpy.random.Generator
    generator = default_rng()
    func = getattr(generator, numpy_distribution_function_str)
    return _replace_with_function(data, keys, func, pass_self_to_func=False, *distribution_args, **distribution_kwargs)


@check_input_type
def reduce_to_mean(data: [dict], keys):
    return _replace_with_aggregate(data, keys, statistics.mean)


@check_input_type
def reduce_to_median(data: [dict], keys):
    return _replace_with_aggregate(data, keys, statistics.median)


def _reset_value(value):
    if isinstance(value, str):
        return ""
    elif isinstance(value, Iterable):
        return []
    elif isinstance(value, int):
        return None
    else:
        return None


def _replace_with_function(data: [dict], keys_to_apply_to, replace_func: Callable, pass_self_to_func=True, *func_args,
                           **func_kwargs):
    if isinstance(keys_to_apply_to, str):
        keys_to_apply_to = [keys_to_apply_to]

    for item in data:
        for key in keys_to_apply_to:
            try:
                if pass_self_to_func:
                    prepped_func = partial(replace_func, item[key])
                else:
                    prepped_func = replace_func
                item[key] = prepped_func(*func_args, **func_kwargs)
            except KeyError:
                pass
    return data


def _replace_with_aggregate(data: [dict], keys_to_aggregate, aggregator: Callable):
    for key in keys_to_aggregate:
        avg = aggregator([item[key] for item in data])
        for item in data:
            item[key] = avg
    return data


def _hashing_wrapper(value, hash_algorithm, salt=None, digest_to_bytes=False):
    value_str = str(value)
    if salt:
        value_str = value_str + str(salt)

    bytes_rep = value_str.encode('utf8')

    if digest_to_bytes:
        return hash_algorithm(bytes_rep).digest()
    else:
        return hash_algorithm(bytes_rep).hexdigest()
