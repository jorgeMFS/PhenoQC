import unittest
import os
import pandas as pd
import tempfile
from src.input import read_csv, read_tsv, read_json, load_data

class TestInputModule(unittest.TestCase):
    def setUp(self):
        self.examples_dir = tempfile.mkdtemp()
        self.csv_file = os.path.join(self.examples_dir, 'sample_data.csv')
        self.tsv_file = os.path.join(self.examples_dir, 'sample_data.tsv')
        self.json_file = os.path.join(self.examples_dir, 'sample_data.json')

        # Create sample CSV file
        with open(self.csv_file, 'w') as f:
            f.write("SampleID,Age,Gender,Phenotype,Measurement\n")
            f.write("S001,34,Male,Hypertension,120\n")
            f.write("S002,28,Female,Diabetes,85\n")
            f.write("S003,45,Other,Asthma,95\n")
            f.write("S004,30,Male,Hypertension,\n")  # Missing Measurement

        # Create sample TSV and JSON files
        df = pd.read_csv(self.csv_file)
        df.to_csv(self.tsv_file, sep='\t', index=False)
        df.to_json(self.json_file, orient='records', lines=False)

    def tearDown(self):
        # Remove temporary directory and its contents
        for file in [self.csv_file, self.tsv_file, self.json_file]:
            if os.path.exists(file):
                os.remove(file)
        os.rmdir(self.examples_dir)

    def test_read_csv(self):
        reader = read_csv(self.csv_file)
        self.assertIsInstance(reader, pd.io.parsers.TextFileReader)
        df = next(reader)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 4)

    def test_read_tsv(self):
        reader = read_tsv(self.tsv_file)
        self.assertIsInstance(reader, pd.io.parsers.TextFileReader)
        df = next(reader)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 4)

    def test_read_json(self):
        reader = read_json(self.json_file, chunksize=2)
        self.assertIsNotNone(reader)
        # Collect all chunks into a list for testing
        chunks = list(reader)
        self.assertEqual(len(chunks), 2)  # Expecting 2 chunks (2 records each)
        for chunk in chunks:
            self.assertIsInstance(chunk, pd.DataFrame)
            self.assertTrue(len(chunk) <= 2)

    def test_load_data_csv(self):
        reader = load_data(self.csv_file, 'csv', chunksize=2)
        self.assertIsInstance(reader, pd.io.parsers.TextFileReader)
        df = next(reader)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    def test_load_data_tsv(self):
        reader = load_data(self.tsv_file, 'tsv', chunksize=2)
        self.assertIsInstance(reader, pd.io.parsers.TextFileReader)
        df = next(reader)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    def test_load_data_json(self):
        reader = load_data(self.json_file, 'json', chunksize=2)
        self.assertIsNotNone(reader)
        chunks = list(reader)
        self.assertEqual(len(chunks), 2)  # Expecting 2 chunks (2 records each)
        for chunk in chunks:
            self.assertIsInstance(chunk, pd.DataFrame)
            self.assertTrue(len(chunk) <= 2)

    def test_load_data_unsupported(self):
        with self.assertRaises(ValueError):
            load_data('example.txt', 'txt')

if __name__ == '__main__':
    unittest.main()
