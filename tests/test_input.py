import unittest
import os
import pandas as pd
from src.input import read_csv, read_tsv, read_json, load_data

class TestInputModule(unittest.TestCase):
    def setUp(self):
        self.csv_file = 'examples/sample_data.csv'
        self.tsv_file = 'examples/sample_data.tsv'
        self.json_file = 'examples/sample_data.json'
        # Create sample TSV and JSON files
        df = read_csv(self.csv_file)
        df.to_csv(self.tsv_file, sep='\t', index=False)
        df.to_json(self.json_file, orient='records', lines=False)

    def tearDown(self):
        os.remove(self.tsv_file)
        os.remove(self.json_file)

    def test_read_csv(self):
        df = read_csv(self.csv_file)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 4)

    def test_read_tsv(self):
        df = read_tsv(self.tsv_file)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 4)

    def test_read_json(self):
        data = read_json(self.json_file)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 4)

    def test_load_data_csv(self):
        data = load_data(self.csv_file, 'csv')
        self.assertIsInstance(data, pd.DataFrame)

    def test_load_data_tsv(self):
        data = load_data(self.tsv_file, 'tsv')
        self.assertIsInstance(data, pd.DataFrame)

    def test_load_data_json(self):
        data = load_data(self.json_file, 'json')
        self.assertIsInstance(data, list)

    def test_load_data_unsupported(self):
        with self.assertRaises(ValueError):
            load_data('example.txt', 'txt')

if __name__ == '__main__':
    unittest.main()