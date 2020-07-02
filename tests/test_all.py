import unittest

from ddt import ddt, data, unpack

from data_minimization_tools import reduce_to_median


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


if __name__ == '__main__':
    unittest.main()
