import unittest
import os
import json
from src.batch_processing import batch_process

class TestBatchProcessingModule(unittest.TestCase):
    def setUp(self):
        # Create sample data files
        self.schema_file = 'schemas/pheno_schema.json'
        os.makedirs('schemas', exist_ok=True)
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
                    "Measurement": {"type": "number"}
                },
                "required": ["SampleID", "Age", "Gender", "Phenotype"],
                "additionalProperties": False
            }, f)
        
        self.mapping_file = 'examples/sample_mapping.json'
        with open(self.mapping_file, 'w') as f:
            json.dump({
                "Hypertension": "HP:0000822",
                "Diabetes": "HP:0001627",
                "Asthma": "HP:0002090"
            }, f)

        self.sample_data_file = 'examples/sample_data.csv'
        os.makedirs('examples', exist_ok=True)
        with open(self.sample_data_file, 'w') as f:
            f.write("SampleID,Age,Gender,Phenotype,Measurement\n")
            f.write("S001,34,Male,Hypertension,120\n")
            f.write("S002,28,Female,Diabetes,85\n")
            f.write("S003,45,Other,Asthma,95\n")
            f.write("S004,30,Male,Hypertension,\n")  # Missing Measurement

    def tearDown(self):
        # Remove created files and directories
        os.remove(self.schema_file)
        os.remove(self.mapping_file)
        os.remove(self.sample_data_file)
        if os.path.exists('reports'):
            for file in os.listdir('reports'):
                os.remove(os.path.join('reports', file))
            os.rmdir('reports')

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