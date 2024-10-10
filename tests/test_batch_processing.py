import unittest
import os
import json
import tempfile
from src.batch_processing import batch_process

class TestBatchProcessingModule(unittest.TestCase):
    def setUp(self):
        # Create temporary directories for schema and examples
        self.schema_dir = tempfile.mkdtemp()
        self.examples_dir = tempfile.mkdtemp()

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
        
        self.mapping_file = os.path.join(self.examples_dir, 'sample_mapping.json')
        with open(self.mapping_file, 'w') as f:
            json.dump([
                {"name": "Hypertension", "id": "HP:0000822"},
                {"name": "Diabetes", "id": "HP:0001627"},
                {"name": "Asthma", "id": "HP:0002090"}
            ], f)

        self.sample_data_file = os.path.join(self.examples_dir, 'sample_data.csv')
        with open(self.sample_data_file, 'w') as f:
            f.write("SampleID,Age,Gender,Phenotype,Measurement\n")
            f.write("S001,34,Male,Hypertension,120\n")
            f.write("S002,28,Female,Diabetes,85\n")
            f.write("S003,45,Other,Asthma,95\n")
            f.write("S004,30,Male,Hypertension,\n")  # Missing Measurement

    def tearDown(self):
        # Remove temporary directories and their contents
        for dir_path in [self.schema_dir, self.examples_dir]:
            for file in os.listdir(dir_path):
                os.remove(os.path.join(dir_path, file))
            os.rmdir(dir_path)

    def test_batch_process(self):
        results = batch_process(
            files=[self.sample_data_file],
            file_type='csv',
            schema_path=self.schema_file,
            hpo_terms_path=self.mapping_file,
            custom_mappings_path=None,
            impute_strategy='mean'
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['status'], 'Processed')
        self.assertIsNone(results[0]['error'])
        # Check if report and processed data exist
        report_path = os.path.join('reports', 'sample_data_report.pdf')
        processed_data_path = os.path.join('reports', 'sample_data.csv')
        self.assertTrue(os.path.exists(report_path))
        self.assertTrue(os.path.exists(processed_data_path))

if __name__ == '__main__':
    unittest.main()