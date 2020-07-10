import inspect
import os
import unittest

import pandas as pd
from ddt import ddt, data, unpack, file_data
from fitparse import FitFile

from data_minimization_tools import reduce_to_median, reduce_to_nearest_value, do_fancy_things


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


    @file_data("data/cvdi.yml")
    def test_yml(self, fitfile_path, expected):
        data, key_mapping = _preprocess_fitfile(os.path.join(get_script_directory(), fitfile_path))

        result = do_fancy_things(data, key_mapping)
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
