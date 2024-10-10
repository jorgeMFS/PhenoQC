import unittest
import os
import pandas as pd
from src.validation import validate_schema, check_required_fields, check_data_types, perform_consistency_checks
import json
import tempfile

class TestValidationModule(unittest.TestCase):
    def setUp(self):
        self.schema_file = tempfile.mktemp(suffix='.json')
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

        self.valid_data = {
            "SampleID": "S005",
            "Age": 29,
            "Gender": "Female",
            "Phenotype": "Obesity",
            "Measurement": 200
        }
        self.invalid_data = {
            "SampleID": "S006",
            "Age": -5,  # Invalid age
            "Gender": "Unknown",  # Invalid gender
            "Phenotype": "Anemia",
            "Measurement": None  # Missing Measurement as null (allowed)
        }
        self.df = pd.DataFrame([self.valid_data, self.invalid_data])

    def tearDown(self):
        if os.path.exists(self.schema_file):
            os.remove(self.schema_file)

    def test_validate_schema_valid(self):
        with open(self.schema_file, 'r') as f:
            schema = json.load(f)
        is_valid, error = validate_schema(self.valid_data, schema)
        self.assertTrue(is_valid)
        self.assertIsNone(error)

    def test_validate_schema_invalid(self):
        with open(self.schema_file, 'r') as f:
            schema = json.load(f)
        is_valid, error = validate_schema(self.invalid_data, schema)
        self.assertFalse(is_valid)
        self.assertIsNotNone(error)

    def test_check_required_fields(self):
        required_fields = ["SampleID", "Age", "Gender", "Phenotype", "Measurement"]
        missing = check_required_fields(self.df, required_fields)
        self.assertEqual(len(missing), 0)  # Ensure no fields are missing

    def test_check_data_types(self):
        expected_types = {
            "SampleID": "string",
            "Age": "number",
            "Gender": "string",
            "Phenotype": "string",
            "Measurement": ["number", "null"]
        }
        mismatches = check_data_types(self.df, expected_types)
        self.assertEqual(len(mismatches), 0)

    def test_perform_consistency_checks(self):
        inconsistencies = perform_consistency_checks(self.df)
        self.assertIn("Negative values found in column 'Age'.", inconsistencies)

if __name__ == '__main__':
    unittest.main()