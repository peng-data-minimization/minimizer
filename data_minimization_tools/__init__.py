import hashlib
import statistics
from collections import Iterable
from functools import partial
from typing import Callable

from numpy.random import default_rng

from .utils import check_input_type


@check_input_type
def drop_keys(data: [dict], keys):
    return _replace_with_function(data, keys, _reset_value)


@check_input_type
def replace_with(data: [dict], replacements: dict):
    """
    Receives a 1:1 mapping of original value to new value and replaces the original values accordingly. This
    corresponds to CN-Protect's DataHierarchy.
    :param data:
    :param replacements:
    :return:
    """
    getitem = lambda mapping, key: mapping[key]
    return _replace_with_function(data, replacements, getitem, pass_self_to_func=True,
                                  replacements=replacements)


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


@check_input_type
def reduce_to_nearest_value(data: [dict], keys, step_width=10):
    return _replace_with_function(data, keys, _get_nearest_value, step_width=step_width)


def _reset_value(value):
    if isinstance(value, str):
        return ""
    elif isinstance(value, Iterable):
        return []
    elif isinstance(value, int):
        return None
    else:
        return None


def _get_nearest_value(value, step_width):
    steps = value // step_width
    return min(steps * step_width, (steps + 1) * step_width, key=lambda new_value: abs(new_value - value))


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
