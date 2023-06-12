import unittest.mock
from interface import main
from io import StringIO
from unittest.mock import patch


class TestAnalysis(unittest.TestCase):
    maxDiff = None

    @patch('builtins.input', side_effect=['2021-04-22', '2022-05-22'])
    def test_main_1(self, mock_input):
        with patch('sys.stdout', new=StringIO()) as fake_output:
            main()

    @patch('builtins.input', side_effect=['2020-01-01', '2021-01-01'])
    def test_main_2(self, mock_input):
        with patch('sys.stdout', new=StringIO()) as fake_output:
            main()

    @patch('builtins.input', side_effect=['2016-01-01', '2018-10-22'])
    def test_main_3(self, mock_input):
        with patch('sys.stdout', new=StringIO()) as fake_output:
            main()
