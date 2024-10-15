import unittest
import os
import json
import tempfile
from src.batch_processing import batch_process

class TestBatchProcessingModule(unittest.TestCase):
    def setUp(self):
        # Create temporary directories for schema and examples
        self.schema_dir = tempfile.mkdtemp()
        self.mapping_dir = tempfile.mkdtemp()

        # Create a temporary configuration file
        self.config_file = os.path.join(self.schema_dir, 'config.yaml')
        with open(self.config_file, 'w') as f:
            f.write("""
ontologies:
  HPO:
    name: Human Phenotype Ontology
    file: {}
default_ontology: HPO
""".format(os.path.join(self.mapping_dir, 'sample_mapping.json')))

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
        
        self.mapping_file = os.path.join(self.mapping_dir, 'sample_mapping.json')
        with open(self.mapping_file, 'w') as f:
            json.dump([
                {"id": "HP:0000822", "name": "Hypertension", "synonyms": []},
                {"id": "HP:0001627", "name": "Diabetes", "synonyms": []},
                {"id": "HP:0002090", "name": "Asthma", "synonyms": []}
            ], f)

        self.sample_data_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv').name
        with open(self.sample_data_file, 'w') as f:
            f.write("SampleID,Age,Gender,Phenotype,Measurement\n")
            f.write("S001,34,Male,Hypertension,120\n")
            f.write("S002,28,Female,Diabetes,85\n")
            f.write("S003,45,Other,Asthma,95\n")
            f.write("S004,30,Male,Hypertension,\n")  # Missing Measurement

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
            config_path=self.config_file,  # Added config_path argument
            unique_identifiers=self.unique_identifiers,
            custom_mappings_path=None,
            impute_strategy='mean',
            output_dir=self.output_dir
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['status'], 'Processed')
        self.assertIsNone(results[0]['error'])
        
        # Check if report and processed data exist
        report_path = os.path.join(self.output_dir, 'tmpxxxxxx_report.pdf')  # The filename will depend on temp file
        processed_data_path = os.path.join(self.output_dir, 'tmpxxxxxx.csv')
        
        # Since tempfile creates unique names, dynamically determine report and CSV files
        base_filename = os.path.splitext(os.path.basename(self.sample_data_file))[0]
        report_path = os.path.join(self.output_dir, f"{base_filename}_report.pdf")
        processed_data_path = os.path.join(self.output_dir, f"{base_filename}.csv")

        self.assertTrue(os.path.exists(report_path))
        self.assertTrue(os.path.exists(processed_data_path))

if __name__ == '__main__':
    unittest.main()
