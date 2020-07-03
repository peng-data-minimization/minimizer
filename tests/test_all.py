import unittest

from ddt import ddt, data, unpack

from data_minimization_tools import reduce_to_median, reduce_to_nearest_value


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


if __name__ == '__main__':
    unittest.main()
