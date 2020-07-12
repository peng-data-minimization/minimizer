import csv
import inspect
import os
import unittest

import pandas as pd
from cn.protect.hierarchy import OrderHierarchy
from ddt import ddt, data, unpack, file_data
from fitparse import FitFile

from data_minimization_tools import reduce_to_median, reduce_to_nearest_value, drop_keys
from data_minimization_tools.cvdi import anonymize_journey
from data_minimization_tools.utils.generate_config import generate_kanon_config


@ddt
class MyTestCase(unittest.TestCase):
    @unpack
    @data({
        "test_data": [
            {"A": 5, "B": 5, "C": 7},
            {"A": 5, "B": 4, "C": 7},
            {"A": 5, "B": 6, "C": 7},
            {"A": 5, "B": 12, "C": 7},
            {"A": 5, "B": 0, "C": 7}],
        "expected": [
            {"A": 5, "B": 5, "C": 7},
            {"A": 5, "B": 5, "C": 7},
            {"A": 5, "B": 5, "C": 7},
            {"A": 5, "B": 5, "C": 7},
            {"A": 5, "B": 5, "C": 7}]})
    def test_median(self, test_data, expected):
        self.assertEqual(reduce_to_median(test_data, "B"), expected)
        self.assertEqual(reduce_to_median(test_data, ["B"]), expected)

    @unpack
    @data({
        "test_data": [
            {"A": 5, "B": 5, "C": 7},
            {"A": 5, "B": 4, "C": 7},
            {"A": 5, "B": 6.2, "C": 7},
            {"A": 5, "B": -11, "C": 7},
            {"A": 5, "B": 0, "C": 7}],
        "expected": [
            {"A": 5, "B": 6, "C": 7},
            {"A": 5, "B": 3, "C": 7},
            {"A": 5, "B": 6, "C": 7},
            {"A": 5, "B": -12, "C": 7},
            {"A": 5, "B": 0, "C": 7}]})
    def test_nearest_number(self, test_data, expected):
        self.assertEqual(reduce_to_nearest_value(test_data, ["B"], step_width=3), expected)

    @unpack
    @data({
        "test_data": [
            {"A": 5, "B": 4},
            {"A": 5, "B": 4, "C": {"A": "foo", "B": 4}},
            {"A": 5, "B": 4, "C": {"A": "foo", "B": 4, "C": {"A": [1, 2, 3], "B": 4}}},
            {"A": 5, "B": 4, "C": [{"A": "foo", "B": 4}, {"A": [1, 2, 3], "B": 4}]}
        ],
        "expected": [
            {"A": None, "B": 4},
            {"A": None, "B": 4, "C": {"A": "", "B": 4}},
            {"A": None, "B": 4, "C": {"A": "", "B": 4, "C": {"A": [], "B": 4}}},
            {"A": None, "B": 4, "C": [{"A": "", "B": 4}, {"A": [], "B": 4}]}
        ]})
    def test_drop_keys(self, test_data, expected):
        self.assertEqual(drop_keys(test_data, ["A", "C.A", "C[].A", "C.C.A"]), expected)
        self.assertEqual(drop_keys(test_data, ["X", "X.X", "C.X", "A[]", "A[].", "A[].X", "X[].X"]), test_data)

    @file_data("data/kanon.yml")
    def test_kanon(self, expected: dict):
        sample = pd.read_csv(os.path.join(get_script_directory(), "data/example-activity.csv"))
        cn_config = {
            "start_latitude": ("quasi", OrderHierarchy("interval", 1, 2, 4)),
            "start_longitude": ("quasi", OrderHierarchy("interval", 1, 2, 4)),
            "external_id": ("identifying", None)
        }

        actual = generate_kanon_config(sample, 2, cn_config)

        for a_function, e_function in zip(actual["tasks"], expected):
            self.assertEqual(a_function["name"][:len(e_function["name"])], e_function["name"])
            del a_function["name"], e_function["name"]
        self.assertEqual(actual, {"tasks": expected})

    @file_data("data/cvdi/direct.yml")
    def test_cvdi_directly(self, input, config_overrides, expected):
        key_mapping = {
            "Latitude": "Latitude",
            "Longitude": "Longitude",
            "Heading": "Heading",
            "Speed": "Speed",
            "Gentime": "Gentime"
        }
        with open(input) as in_file:
            reader = csv.DictReader(in_file)
            data = [{key: float(val) if val != "" else None for key, val in r.items()} for r in reader]
            result = anonymize_journey(data, key_mapping, config_overrides)
        with open(expected) as exp_file:
            reader = csv.DictReader(exp_file)
            expected = [{key: float(val) if val != "" else None for key, val in r.items()} for r in reader]
            self.assertAlmostEqual(result, expected)

    @file_data("data/cvdi/fitfiles.yml")
    def test_cvdi_from_fitfile(self, fitfile_path, expected):
        data, key_mapping = _preprocess_fitfile(os.path.join(get_script_directory(), fitfile_path))

        result = anonymize_journey(data, key_mapping)
        self.assertAlmostEqual(result, expected)


def _preprocess_fitfile(file_path):
    ff = FitFile(file_path)
    data = []
    key_mapping = {
        "position_lat": "Latitude",
        "position_long": "Longitude",
        "heart_rate": "Heading",
        "enhanced_speed": "Speed",
        "timestamp": "Gentime"
    }
    for record in ff.get_messages('record'):
        record_dict = {metric.name: (metric.value if metric.name != "timestamp" else metric.raw_value) for
                       metric in record}
        if all(required_key in record_dict.keys() for required_key in key_mapping):
            record_dict.update({
                'activityId': 7,
                'type': 'fitfile_upload',
                "position_lat": semicircles_to_degrees(
                    record_dict["position_lat"]),
                "position_long": semicircles_to_degrees(
                    record_dict["position_long"])
            })
            data.append(record_dict)
        else:
            print("skipping")
    return data, key_mapping


def semicircles_to_degrees(semicircles):
    return semicircles * 180 / 2 ** 31


def get_script_directory():
    return os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))


if __name__ == '__main__':
    unittest.main()
