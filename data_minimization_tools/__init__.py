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
from .utils.generate_config import generate_cvdi_config


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


def _prepare_dicts_for_cvdi_consumption(data: [dict], geodata_key_map: dict):
    """
    For several dicts, rename columns relevant to geodata so that they are understood by our geodata anonymization tool,
    and add columns that the tool requires to be present in the order the tool expects. (The last part might not be
    necessary.)

    *Example*
        ``[{"lat": 14, "lng": 52, "something_else": "foo"}]``

        ↦ ``[{"RxDevice": 1, "Latitude": 14, "Longitude": 52, "Ax": None}]``

    *Rationale*
        The cv-di is very peculiar about the csv format it accepts as input thata. It does not support setting field
        names when using the cli, but only when using the GUI: compare https://github.com/usdot-its-jpo-data-portal/privacy-protection-application/blob/fd59e3e42842fb80d579d7efa2dd6f1349e67899/cv-gui-electron/cpp/src/cvdi_nm.cc#L817
        with https://github.com/usdot-its-jpo-data-portal/privacy-protection-application/blob/fd59e3e42842fb80d579d7efa2dd6f1349e67899/cl-tool/src/config.cpp#L306

        Maybe, if we used the cvdi_nm (node module) instead of the cli binary, this would work?

    :param data: input data as list of dicts
    :param geodata_key_map: Map of keys
    :return:
    """
    required_keys = ["RxDevice", "FileId", "TxDevice", "Gentime", "TxRandom", "MsgCount", "DSecond", "Latitude",
                     "Longitude", "Elevation", "Speed", "Heading", "Ax", "Ay", "Az", "Yawrate", "PathCount",
                     "RadiusOfCurve", "Confidence"]
    return [{
        **{required_key: None for required_key in required_keys},
        # Set an arbitrary value for all points of the journey to mark them as being part of that journey.
        # FIXME Are all of these really required?
        "TxDevice": 1,
        "RxDevice": 1,
        "FileId": 1,
        **{cvdi_key: original_item[original_key] for original_key, cvdi_key in geodata_key_map.items()}
    } for original_item in data]


def _revert_dict_preparation_for_cvdi_consumption(cvdi_output: [dict], original_data: [dict], geodata_key_map: dict) -> \
        [dict]:
    """
    Undo the re-mapping and dropping of keys that was applied to make the data ingestible by the cv-di. For details on
    that, see :func:`_prepare_dicts_for_cvdi_consumption`.

    The cv-di output will contain a lot less items than the original data did. Joins the to lists based on their
    timestamp, and drop lines in input data that are not contained in the cv-di output.

    :param cvdi_output:
    :param original_data:
    :param geodata_key_map:
    :return:
    """
    cvdi_key_to_join_by = "Gentime"
    original_key_to_join_by = next(original_key for original_key, cvdi_key in geodata_key_map.items()
                                   if cvdi_key == cvdi_key_to_join_by)

    def is_join_match(cvdi_item, original_item):
        return cvdi_item[cvdi_key_to_join_by] == original_item[original_key_to_join_by]

    # gentime is unique for one journey --> inner one to one join, throw away remaining original_data
    joint = [(
        next(original_item for original_item in original_data if is_join_match(cvdi_item, original_item)),
        cvdi_item
    ) for cvdi_item in cvdi_output]

    return [{
        **original_item,
        **{original_key: cvdi_processed_item[cvdi_key] for original_key, cvdi_key in geodata_key_map.items()}
    } for original_item, cvdi_processed_item in joint]


@check_input_type
def do_fancy_things(data: [dict], original_to_cvdi_key: dict, cvdi_overrides=None):
    if cvdi_overrides is None:
        cvdi_overrides = {}

    REQUIRED_KEYS = {"Latitude", "Longitude", "Heading", "Speed", "Gentime"}
    # also trip_id. Heading should be generated later?
    if set(original_to_cvdi_key.values()) != REQUIRED_KEYS:
        warn(textwrap.dedent(f"""
                The following keys should be defined for the cvdi library: 
                {REQUIRED_KEYS}, 
                but got the following:
                {original_to_cvdi_key.values()}
                mapping to: 
                {original_to_cvdi_key}.
                This might lead to wonky results or a crash."""),
             RuntimeWarning)

    script_abs_directory = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    current_working_directory = os.getcwd()

    cvdi_config_dir = f"{current_working_directory}/cvdi-conf"
    cvdi_out_dir = f"{current_working_directory}/cvdi-consume"

    for dirname in cvdi_config_dir, cvdi_out_dir:
        try:
            os.mkdir(dirname)
        except FileExistsError:
            pass

    data_for_cvdi = _prepare_dicts_for_cvdi_consumption(data, original_to_cvdi_key)
    with open(os.path.join(cvdi_config_dir, "THE_FILE.csv"), "w+") as data_file:
        fieldnames = [key for key in data_for_cvdi[0]]
        writer = csv.DictWriter(data_file, fieldnames, dialect=csv.excel)
        writer.writeheader()
        writer.writerows(data_for_cvdi)

    config = generate_cvdi_config(data, original_to_cvdi_key, cvdi_overrides)
    with open(os.path.join(cvdi_config_dir, "config"), "w+") as config_file:
        config_file.write(config)

    # replace c:\\, d:\\, etc, with / and hope things don't break.
    data_file_path = cvdi_config_dir if cvdi_config_dir[0] == "/" else "/" + cvdi_config_dir[3:]
    with open(os.path.join(cvdi_config_dir, "data_file_list"), "w+") as data_file_list_file:
        data_file_list_file.write(os.path.join(data_file_path, "THE_FILE.csv"))

    cvdi_executable_path = os.path.join(script_abs_directory, "bin/cv_di")

    def run_cvdi(binary_path: str):
        call = [binary_path, *_get_cvdi_args(cvdi_config_dir, cvdi_out_dir)]
        print(f"Calling {call}")
        return subprocess.run(call, check=True, capture_output=True)

    try:
        cvdi_process = run_cvdi(cvdi_executable_path)
    except OSError:
        # assuming running on windows
        cvdi_process = run_cvdi(cvdi_executable_path + ".exe")

    if cvdi_process.stderr[-106:-93] == b"0,0,0,0,0,0,0":
        raise Exception(f"CV-DI processed exactly 0 lines, "
                        f"message was: {cvdi_process.stderr.splitlines()[-5]}")
    if cvdi_process.stderr[-95:-93] == b",0":
        raise Exception(f"CV-DI produced exactly 0 points as part of a privacy interval, "
                        f"message was: {cvdi_process.stderr.splitlines()[-5]}")

    processed_data_candidates = [name for name in os.listdir(cvdi_out_dir) if name.endswith(".csv")]

    if len(processed_data_candidates) != 1:
        raise Exception(f"Expected exactly one produced CSV file in {cvdi_out_dir}, found {processed_data_candidates}.")

    processed_data_file_name = os.path.join(cvdi_out_dir, processed_data_candidates[0])

    with open(processed_data_file_name) as csvfile:
        cvdi_processed_data = [{
            # hackily restore original types instead of parsing everything as string
            key: float(val) if val != '' else None for key, val in row.items()
        } for row in csv.DictReader(csvfile)]

    return _revert_dict_preparation_for_cvdi_consumption(cvdi_processed_data, data, original_to_cvdi_key)


def _get_cvdi_args(cvdi_config_dir, cvdi_out_dir) -> Iterable:
    config_file_path = os.path.join(cvdi_config_dir, "config")
    quad_file_path = os.path.join(cvdi_config_dir, "quad")
    data_file_list_file_path = os.path.join(cvdi_config_dir, "data_file_list")
    return ["-n",
            "-c", config_file_path,
            "-q", quad_file_path,
            "-o", cvdi_out_dir,
            "-k", cvdi_out_dir,
            data_file_list_file_path]


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
