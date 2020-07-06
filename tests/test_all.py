import unittest

import pandas as pd
from cn.protect.hierarchy import OrderHierarchy
from ddt import ddt, data, unpack, file_data

from data_minimization_tools import reduce_to_median, reduce_to_nearest_value
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

    @file_data("data/kanon.yml")
    def test_kanon(self, expected: dict):
        sample = pd.read_csv("data/example-activity.csv")
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


if __name__ == '__main__':
    unittest.main()
