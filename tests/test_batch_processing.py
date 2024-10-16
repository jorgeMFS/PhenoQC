import unittest
import os
import json
import tempfile
from src.batch_processing import batch_process
from src.configuration import load_config
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
    file: {os.path.join(self.mapping_dir, 'sample_mapping.json')}
default_ontology: HPO
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

        # Create sample mapping file
        self.mapping_file = os.path.join(self.mapping_dir, 'sample_mapping.json')
        with open(self.mapping_file, 'w') as f:
            json.dump([
                {"id": "HP:0000822", "name": "Hypertension", "synonyms": []},
                {"id": "HP:0001627", "name": "Diabetes", "synonyms": []},
                {"id": "HP:0002090", "name": "Asthma", "synonyms": []}
            ], f)

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
        # Remove temporary directories and their contents
        for dir_path in [self.schema_dir, self.mapping_dir, self.output_dir]:
            if os.path.exists(dir_path):
                for file in os.listdir(dir_path):
                    file_path = os.path.join(dir_path, file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                os.rmdir(dir_path)

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
        self.assertEqual(results[0]['status'], 'Processed')
        self.assertIsNone(results[0]['error'])

        # Check if report and processed data exist
        base_filename = os.path.splitext(os.path.basename(self.sample_data_file))[0]
        report_path = os.path.join(self.output_dir, f"{base_filename}_report.pdf")
        processed_data_path = os.path.join(self.output_dir, f"{base_filename}.csv")
        flagged_records_path = os.path.join(self.output_dir, f"{base_filename}_flagged_records.csv")

        self.assertTrue(os.path.exists(report_path))
        self.assertTrue(os.path.exists(processed_data_path))
        self.assertTrue(os.path.exists(flagged_records_path))

        # Load processed data and check if missing values were imputed correctly
        df_processed = pd.read_csv(processed_data_path)
        self.assertFalse(df_processed.isnull().any().any())  # No missing values

        # Verify that 'MissingDataFlag' column exists
        self.assertIn('MissingDataFlag', df_processed.columns)
        self.assertEqual(df_processed['MissingDataFlag'].sum(), 0)  # Should be 0 after imputation

    def test_load_config_imputation_strategies(self):
        config = load_config(self.config_file)
        self.assertIn('imputation_strategies', config)
        self.assertEqual(config['imputation_strategies']['Age'], 'median')
        self.assertEqual(config['imputation_strategies']['Gender'], 'mode')
        self.assertEqual(config['imputation_strategies']['Measurement'], 'mean')

if __name__ == '__main__':
    unittest.main()
