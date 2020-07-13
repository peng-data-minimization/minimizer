import csv
import inspect
import os
import subprocess
import textwrap
from typing import Iterable
from warnings import warn

from data_minimization_tools.utils import check_input_type
from data_minimization_tools.utils import generate_cvdi_config

REQUIRED_KEYS = {"Latitude", "Longitude", "Heading", "Speed",
                 "Gentime"}  # Heading should be generated / calculated later?


@check_input_type
def anonymize_journey(data: [dict], original_to_cvdi_key: dict, config_overrides=None):
    if config_overrides is None:
        config_overrides = {}

    validate_key_mapping(original_to_cvdi_key)

    script_abs_directory = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    current_working_directory = os.getcwd()
    executable_path = os.path.join(script_abs_directory, "bin/cv_di")
    config_dir, out_dir = make_directories(current_working_directory)

    write_data(config_dir, data, original_to_cvdi_key)
    write_config(config_dir, config_overrides, data, original_to_cvdi_key)

    cvdi_process = run_cvdi(executable_path, config_dir, out_dir)
    check_process_logs(cvdi_process)

    processed_data = read_results(out_dir)

    return _revert_dict_preparation_for_cvdi_consumption(processed_data, data, original_to_cvdi_key)


def validate_key_mapping(original_to_cvdi_key):
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


def read_results(out_dir):
    processed_data_candidates = [name for name in os.listdir(out_dir) if name.endswith(".csv")]
    if len(processed_data_candidates) != 1:
        raise Exception(f"Expected exactly one produced CSV file in {out_dir}, found {processed_data_candidates}.")
    processed_data_file_name = os.path.join(out_dir, processed_data_candidates[0])
    with open(processed_data_file_name) as csvfile:
        cvdi_processed_data = [{
            # hackily restore original types instead of parsing everything as string
            key: float(val) if val != '' else None for key, val in row.items()
        } for row in csv.DictReader(csvfile)]
    return cvdi_processed_data


def run_cvdi(executable_path, config_dir, out_dir):
    def _run_cvdi(binary_path: str):
        call = [binary_path, *_get_cvdi_args(config_dir, out_dir)]
        print(f"Calling {call}")
        return subprocess.run(call, check=True, capture_output=True)

    try:
        cvdi_process = _run_cvdi(executable_path)
    except OSError:
        # assuming running on windows
        cvdi_process = _run_cvdi(executable_path + ".exe")
    return cvdi_process


def make_directories(current_working_directory):
    config_dir = f"{current_working_directory}/cvdi-conf"
    out_dir = f"{current_working_directory}/cvdi-consume"
    for dirname in config_dir, out_dir:
        try:
            os.mkdir(dirname)
        except FileExistsError:
            pass
    return config_dir, out_dir


def write_data(config_dir, data, original_to_cvdi_key):
    data_for_cvdi = _prepare_dicts_for_cvdi_consumption(data, original_to_cvdi_key)
    with open(os.path.join(config_dir, "THE_FILE.csv"), "w+") as data_file:
        fieldnames = [key for key in data_for_cvdi[0]]
        writer = csv.DictWriter(data_file, fieldnames, dialect=csv.excel)
        writer.writeheader()
        writer.writerows(data_for_cvdi)


def write_config(config_dir, cvdi_overrides, data, original_to_cvdi_key):
    config = generate_cvdi_config(data, original_to_cvdi_key, cvdi_overrides)
    with open(os.path.join(config_dir, "config"), "w+") as config_file:
        config_file.write(config)
    # replace c:\\, d:\\, etc, with / and hope things don't break.
    data_file_path = config_dir if config_dir[0] == "/" else "/" + config_dir[3:]
    with open(os.path.join(config_dir, "data_file_list"), "w+") as data_file_list_file:
        data_file_list_file.write(os.path.join(data_file_path, "THE_FILE.csv"))


def check_process_logs(process):
    if process.stderr[-106:-93] == b"0,0,0,0,0,0,0":
        raise Exception(f"CV-DI processed exactly 0 lines, "
                        f"message was: {process.stderr.splitlines()[-5]}")
    if process.stderr[-95:-93] == b",0":
        raise Exception(f"CV-DI produced exactly 0 points as part of a privacy interval, "
                        f"message was: {process.stderr.splitlines()[-5]}")


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
    all_csv_fields = ["RxDevice", "FileId", "TxDevice", "Gentime", "TxRandom", "MsgCount", "DSecond", "Latitude",
                      "Longitude", "Elevation", "Speed", "Heading", "Ax", "Ay", "Az", "Yawrate", "PathCount",
                      "RadiusOfCurve", "Confidence"]
    return [{
        **{key: None for key in all_csv_fields},
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


def _get_cvdi_args(config_dir, out_dir) -> Iterable:
    config_file_path = os.path.join(config_dir, "config")
    quad_file_path = os.path.join(config_dir, "quad")
    data_file_list_file_path = os.path.join(config_dir, "data_file_list")
    return ["-n",
            "-c", config_file_path,
            "-q", quad_file_path,
            "-o", out_dir,
            "-k", out_dir,
            data_file_list_file_path]
