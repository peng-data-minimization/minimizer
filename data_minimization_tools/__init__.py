import hashlib
import statistics
from collections.abc import Iterable
from functools import partial
from typing import Callable

from numpy.random import default_rng

from .utils import check_input_type


@check_input_type
def drop_keys(data: [dict], keys):
    """
    Removes the data for specific keys (does not drop the key form the dictionary!

    :param data: input data as list of dicts
    :param keys: list of keys whose values should be removed
    :return: cleaned list of dicts
    """
    return _replace_with_function(data, keys, _reset_value)


@check_input_type
def replace_with(data: [dict], replacements: dict):
    """
    Receives a 1:1 mapping of original value to new value and replaces the original values accordingly. This
    corresponds to CN-Protect's DataHierarchy.

    :param data: input data as list of dicts
    :param replacements: 1:1 mapping
    :return: cleaned list of dicts
    """
    getitem = lambda mapping, key: mapping[key]
    return _replace_with_function(data, replacements, getitem, pass_self_to_func=True,
                                  replacements=replacements)


@check_input_type
def hash_keys(data: [dict], keys, hash_algorithm=hashlib.sha256, salt=None, digest_to_bytes=False):
    """
    Hashes data for specific keys.

    :param data: input data as list of dicts
    :param keys: list of keys whose values should be hashed
    :param hash_algorithm: the hashalgorith to apply. Can be any hashlib algorith or any function that behaves similarly
    :param salt: the salt to use
    :param digest_to_bytes: whether result should be bytes. If False, result is of type string
    :return: cleaned list of dicts
    """
    return _replace_with_function(data, keys, _hashing_wrapper, hash_algorithm=hash_algorithm,
                                  digest_to_bytes=digest_to_bytes, salt=salt)


@check_input_type
def replace_with_distribution(data: [dict], keys, numpy_distribution_function_str='standard_normal', *distribution_args,
                              **distribution_kwargs):
    """
    Replaces data for specific keys with data generated from a distribution.

    :param data: input data as list of dicts
    :param keys: list of keys whose values should be replaced
    :param numpy_distribution_function_str: for possible distribution functions see
                                            `here. <https://numpy.org/doc/stable/reference/random/generator.html#numpy.random.Generator>`_
                                            Pass the function as string
    :param distribution_args: additional args that the chosen function requires
    :param distribution_kwargs: additional kwargs that the chosen function requires
    :return: cleaned list of dicts
    """

    generator = default_rng()
    func = getattr(generator, numpy_distribution_function_str)
    return _replace_with_function(data, keys, func, pass_self_to_func=False, *distribution_args, **distribution_kwargs)


@check_input_type
def reduce_to_mean(data: [dict], keys):
    """
    Reduce all values for the given key to the mean across all values of the input data list

    :param data: input data as list of dicts
    :param keys: list of keys whose values should be replaced
    :return: cleaned list of dicts. Note, that this function returns as many items as you input.
    """
    return _replace_with_aggregate(data, keys, statistics.mean)


@check_input_type
def reduce_to_median(data: [dict], keys):
    """
    Reduce all values for the given key to the median across all values of the input data list

    :param data: input data as list of dicts
    :param keys: list of keys whose values should be replaced
    :return: cleaned list of dicts. Note, that this function returns as many items as you input.
    """
    return _replace_with_aggregate(data, keys, statistics.median)


@check_input_type
def reduce_to_nearest_value(data: [dict], keys, step_width=10):
    """
    Reduce all values for the given key to the nearest value. Think of this as aggregating values as intervals.

    :param data: input data as list of dicts
    :param keys: list of keys whose values should be replaced
    :param step_width: size of the intervals
    :return: cleaned list of dicts. Note, that this function returns as many items as you input.
    """
    return _replace_with_function(data, keys, _get_nearest_value, step_width=step_width)


def _reset_value(value):
    """
    helper function. Sould not be used from the api.

    :param value:
    :return:
    """
    if isinstance(value, str):
        return ""
    elif isinstance(value, Iterable):
        return []
    elif isinstance(value, int):
        return None
    else:
        return None


def _get_nearest_value(value, step_width):
    """
    helper function. Sould not be used from the api.

    :param value:
    :param step_width:
    :return:
    """
    steps = value // step_width
    return min(steps * step_width, (steps + 1) * step_width, key=lambda new_value: abs(new_value - value))


def _replace_with_function(data: [dict], keys_to_apply_to, replace_func: Callable, pass_self_to_func=True, *func_args,
                           **func_kwargs):
    """
    helper function. Sould not be used from the api.

    :param data:
    :param keys_to_apply_to:
    :param replace_func:
    :param pass_self_to_func:
    :param func_args:
    :param func_kwargs:
    :return:
    """
    if not isinstance(data, list):
        return data

    if isinstance(keys_to_apply_to, str):
        keys_to_apply_to = [keys_to_apply_to]

    for item in data:
        for key in keys_to_apply_to:

            if "[]." in key:
                list_key, rest = key.split("[].", 1)
                _replace_with_function(_get(item, list_key), rest, replace_func, pass_self_to_func, *func_args,
                                       **func_kwargs)
                continue
            try:
                if pass_self_to_func:
                    prepped_func = partial(replace_func, _get(item, key))
                else:
                    prepped_func = replace_func
                value = prepped_func(*func_args, **func_kwargs)
                _put(item, key, value)
            except KeyError:
                pass
    return data


def _get(d, keys):
    """
    helper function to get a value from a nested dict with dotted string notation.
    Sould not be used from the api.

    :param d:
    :param keys:
    :return:
    """
    if not isinstance(d, dict):
        return
    if "." in keys:
        key, rest = keys.split(".", 1)
        return _get(d.get(key), rest)
    else:
        return d.get(keys)


def _put(d, keys, value):
    """
    helper function to put a value into a nested dict with dotted string notation.
    Sould not be used from the api.
    :param d:
    :param keys:
    :param value:
    :return:
    """
    if not isinstance(d, dict):
        return
    if "." in keys:
        key, rest = keys.split(".", 1)
        _put(d[key], rest, value)
    else:
        d[keys] = value


def _replace_with_aggregate(data: [dict], keys_to_aggregate, aggregator: Callable):
    """
    helper function. Sould not be used from the api.


    :param data:
    :param keys_to_aggregate:
    :param aggregator:
    :return:
    """
    for key in keys_to_aggregate:
        avg = aggregator([item[key] for item in data])
        for item in data:
            item[key] = avg
    return data


def _hashing_wrapper(value, hash_algorithm, salt=None, digest_to_bytes=False):
    """
    helper function. Sould not be used from the api.

    :param value:
    :param hash_algorithm:
    :param salt:
    :param digest_to_bytes:
    :return:
    """
    value_str = str(value)
    if salt:
        value_str = value_str + str(salt)

    bytes_rep = value_str.encode('utf8')

    if digest_to_bytes:
        return hash_algorithm(bytes_rep).digest()
    else:
        return hash_algorithm(bytes_rep).hexdigest()
