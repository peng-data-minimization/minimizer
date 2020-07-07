import csv
import hashlib
import inspect
import os
import statistics
import subprocess
import textwrap
from collections.abc import Iterable
from functools import partial
from typing import Callable
from warnings import warn

from numpy.random import default_rng

from .utils import check_input_type
from .utils.generate_config import generate_ppa_config


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


def _prepare_dicts_for_ppa_consumption(data: [dict], geodata_key_map: dict):
    """
    For several dicts, rename columns relevant to geodata so that they are understood by our geodata anonymization tool.

    *Example*
        ``[{"lat": 14, "lng": 52, "something_else": "foo"}]``

        ↦ ``[{"Latitude": 14, "Longitude": 52}]``

    *Rationale*
        The privacy-protection-application does not support setting field names when using the cli, but only when using
        the GUI: compare https://github.com/usdot-its-jpo-data-portal/privacy-protection-application/blob/fd59e3e42842fb80d579d7efa2dd6f1349e67899/cv-gui-electron/cpp/src/cvdi_nm.cc#L817
        with https://github.com/usdot-its-jpo-data-portal/privacy-protection-application/blob/fd59e3e42842fb80d579d7efa2dd6f1349e67899/cl-tool/src/config.cpp#L306

        Maybe, if we used the cvdi_nm (node module) instead of the cli binary, this would work?

    :param data:
    :param geodata_key_map: Map of keys
    :return:
    """
    return [{
        ppa_key: original_item[original_key] for original_key, ppa_key in geodata_key_map.items()
    } for original_item in data]


def _revert_dict_preparation_for_ppa_consumption(ppa_output: [dict], original_data: [dict], geodata_key_map: dict) -> \
        [dict]:
    """
    See :func:`_prepare_dicts_for_ppa_consumption`.

    :param ppa_output:
    :param original_data:
    :param geodata_key_map:
    :return:
    """
    if len(ppa_output) != len(original_data):
        raise Exception(f"Data was lost! Original data was {len(original_data)} items, after ppa: {len(ppa_output)}")

    return [{
        **original_item,
        **{original_key: ppa_processed_item[ppa_key] for original_key, ppa_key in geodata_key_map.items()}
    } for original_item, ppa_processed_item in zip(original_data, ppa_output)]


@check_input_type
def do_fancy_things(data: [dict], key_thingy: dict):
    REQUIRED_KEYS = {"Latitude", "Longitude", "Heading", "Speed", "Gentime"}
    # also trip_id. Heading should be generated later?
    if set(key_thingy.values()) != REQUIRED_KEYS:
        warn(textwrap.dedent(f"""
                The following keys should be defined for the ppa library: 
                {REQUIRED_KEYS}, 
                but got the following:
                {key_thingy.values()}
                mapping to: 
                {key_thingy}.
                This might lead to wonky results or a crash."""),
             RuntimeWarning)

    script_abs_directory = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    current_working_directory = os.getcwd()

    ppa_config_dir = f"{current_working_directory}/ppa-conf"
    ppa_out_dir = f"{current_working_directory}/ppa-consume"

    data_for_ppa = _prepare_dicts_for_ppa_consumption(data, key_thingy)
    with open(os.path.join(ppa_config_dir, "THE_FILE.csv"), "w") as data_file:
        fieldnames = [key for key in data_for_ppa[0]]
        writer = csv.DictWriter(data_file, fieldnames)
        writer.writeheader()
        writer.writerows(data_for_ppa)

    config = generate_ppa_config(data, key_thingy)
    with open(os.path.join(ppa_config_dir, "config"), "w") as config_file:
        config_file.write(config)

    with open(os.path.join(ppa_config_dir, "data_file"), "w") as data_file_list_file:
        data_file_list_file.write(os.path.join(ppa_config_dir, "THE_FILE.csv"))

    # ppa_executable_path = os.path.join(script_abs_directory, "bin/cv_di") # Does not work on Windows
    ppa_executable_path = "bin/cv_di"
    subprocess.run([ppa_executable_path, *_get_ppa_args(ppa_config_dir, ppa_out_dir)], check=True)

    processed_data_candidates = [name for name in os.listdir(ppa_out_dir) if name.endswith(".csv")]

    if len(processed_data_candidates) != 1:
        raise Exception("Expected exactly one produced CSV file, found " + str(processed_data_candidates))

    processed_data_file_name = processed_data_candidates[0]

    with open(processed_data_file_name) as csvfile:
        ppa_processed_data = list(csv.DictReader(csvfile))
        return _revert_dict_preparation_for_ppa_consumption(ppa_processed_data, data, key_thingy)


def _get_ppa_args(ppa_config_dir, ppa_out_dir) -> Iterable[str]:
    config_file_path = os.path.join(ppa_config_dir, "config")
    quad_file_path = os.path.join(ppa_config_dir, "quad")
    data_file_list_file_path = os.path.join(ppa_config_dir, "data_file")
    return ["-n",
            "-c", config_file_path,
            "-q", quad_file_path,
            "-o", ppa_out_dir,
            "-k", ppa_out_dir,
            data_file_list_file_path]


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
