import unittest
import os
import json
import tempfile
import hashlib
from unittest.mock import patch

from phenoqc.batch_processing import (
    batch_process,
    unique_output_name,
    convert_nans_to_none_for_string_cols,
    get_file_type,
    _safe_md5_hexdigest,
)
from phenoqc.configuration import load_config
import pandas as pd

class TestBatchProcessingModule(unittest.TestCase):
    def setUp(self):
        # Create temporary directories for schema and mappings
        self.schema_dir = tempfile.mkdtemp()
        self.mapping_dir = tempfile.mkdtemp()

        # Create a temporary configuration file
        self.config_file = os.path.join(self.schema_dir, 'config.yaml')
        with open(self.config_file, 'w') as f:
            f.write(f"""
imputation_strategies:
  Age: median
  Gender: mode
  Measurement: mean
ontologies:
  HPO:
    name: Human Phenotype Ontology
    file: {os.path.join(self.mapping_dir, 'sample_mapping.obo')}
default_ontologies: [HPO]
""")

        # Create schema file
        self.schema_file = os.path.join(self.schema_dir, 'pheno_schema.json')
        with open(self.schema_file, 'w') as f:
            json.dump({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Phenotypic Data Schema",
                "type": "object",
                "properties": {
                    "SampleID": {"type": "string"},
                    "Age": {"type": "number", "minimum": 0},
                    "Gender": {"type": "string", "enum": ["Male", "Female", "Other"]},
                    "Phenotype": {"type": "string"},
                    "Measurement": {"type": ["number", "null"]}
                },
                "required": ["SampleID", "Age", "Gender", "Phenotype"],
                "additionalProperties": False
            }, f)

        # Create sample ontology file in OBO format
        self.mapping_file = os.path.join(self.mapping_dir, 'sample_mapping.obo')
        with open(self.mapping_file, 'w') as f:
            f.write("""
format-version: 1.2
data-version: releases/2021-02-01
ontology: sample

[Term]
id: HP:0000822
name: Hypertension
synonym: "High blood pressure" EXACT []

[Term]
id: HP:0001627
name: Diabetes
synonym: "Sugar disease" EXACT []

[Term]
id: HP:0002090
name: Asthma
synonym: "Bronchial disease" EXACT []
""")

        # Create sample data file
        self.sample_data_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv').name
        with open(self.sample_data_file, 'w') as f:
            f.write("SampleID,Age,Gender,Phenotype,Measurement\n")
            f.write("S001,34,Male,Hypertension,120\n")
            f.write("S002,28,Female,Diabetes,85\n")  # Provided Age
            f.write("S003,45,Other,Asthma,95\n")     # Provided Gender
            f.write("S004,30,Male,Hypertension,\n")  # Missing optional field

        self.unique_identifiers = ['SampleID']

        # Create a temporary output directory
        self.output_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        # Remove temporary directories and their contents
        for dir_path in [self.schema_dir, self.mapping_dir, self.output_dir]:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)

        # Remove sample data file
        if os.path.exists(self.sample_data_file):
            os.remove(self.sample_data_file)

    def test_batch_process(self):
        results = batch_process(
            files=[self.sample_data_file],
            schema_path=self.schema_file,
            config_path=self.config_file,
            unique_identifiers=self.unique_identifiers,
            custom_mappings_path=None,
            impute_strategy='mean',  # Default strategy
            output_dir=self.output_dir
        )
        self.assertEqual(len(results), 1)
        self.assertIn(results[0]['status'], ['Processed', 'ProcessedWithWarnings'])
        if results[0]['error'] is not None:
            self.assertIn("Format validation failed.", results[0]['error'])

        report_path = unique_output_name(self.sample_data_file, self.output_dir, suffix='_report.pdf')
        processed_data_path = unique_output_name(self.sample_data_file, self.output_dir, suffix='.csv')

        self.assertTrue(os.path.exists(report_path), f"QC report not found at {report_path}")
        self.assertTrue(os.path.exists(processed_data_path), f"Processed data not found at {processed_data_path}")

        # Load processed data and check if missing values were imputed correctly
        df_processed = pd.read_csv(processed_data_path)
        # Since 'Measurement' was missing for S004 and imputed with mean (which is (120 + 85 + 95)/3 = 100), check
        self.assertFalse(df_processed['Measurement'].isnull().any(), "Missing values were not imputed correctly.")
        self.assertAlmostEqual(df_processed.loc[df_processed['SampleID'] == 'S004', 'Measurement'].values[0], 100.0, places=1)

        # Verify that 'MissingDataFlag' column exists and is correctly set
        self.assertIn('MissingDataFlag', df_processed.columns, "'MissingDataFlag' column is missing.")
        # After imputation, there should be no flags
        self.assertEqual(df_processed['MissingDataFlag'].sum(), 0, "There are still missing data flags after imputation.")

    def test_load_config_imputation_strategies(self):
        config = load_config(self.config_file)
        self.assertIn('imputation_strategies', config)
        self.assertEqual(config['imputation_strategies']['Age'], 'median')
        self.assertEqual(config['imputation_strategies']['Gender'], 'mode')
        self.assertEqual(config['imputation_strategies']['Measurement'], 'mean')

    def test_get_file_type(self):
        self.assertEqual(get_file_type('data.csv'), 'csv')
        self.assertEqual(get_file_type('data.tsv'), 'tsv')
        self.assertEqual(get_file_type('data.json'), 'json')
        with self.assertRaises(ValueError):
            get_file_type('data.txt')

    def test_convert_nans_to_none_for_string_cols(self):
        df = pd.DataFrame({'name': ['Alice', float('nan')], 'age': [30, float('nan')]})
        schema = {'properties': {'name': {'type': ['string', 'null']}, 'age': {'type': 'number'}}}
        converted = convert_nans_to_none_for_string_cols(df, schema)
        self.assertIsNone(converted.loc[1, 'name'])
        self.assertTrue(pd.isna(converted.loc[1, 'age']))

    def test_unique_output_name_stability(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = '/any/path/sample.json'
            first = unique_output_name(path, tmpdir, suffix='_report.pdf')
            second = unique_output_name(path, tmpdir, suffix='_report.pdf')
            self.assertEqual(first, second)
            self.assertTrue(first.endswith('_json_report.pdf'))

    def test_safe_md5_hexdigest_fallback(self):
        data = b'test-data'
        expected = hashlib.md5(data).hexdigest()
        with patch('hashlib.new', side_effect=TypeError):
            self.assertEqual(_safe_md5_hexdigest(data), expected)
        with patch('hashlib.new', side_effect=TypeError), patch('hashlib.md5', side_effect=ValueError):
            with self.assertRaises(RuntimeError):
                _safe_md5_hexdigest(data)

        # Test with empty data input
        empty_data = b''
        expected_empty = hashlib.md5(empty_data).hexdigest()
        with patch('hashlib.new', side_effect=TypeError):
            self.assertEqual(_safe_md5_hexdigest(empty_data), expected_empty)

if __name__ == '__main__':
    unittest.main()