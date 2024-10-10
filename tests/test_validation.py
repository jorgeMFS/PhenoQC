import unittest
import os
import pandas as pd
from src.validation import validate_schema, check_required_fields, check_data_types, perform_consistency_checks
import json

class TestValidationModule(unittest.TestCase):
    def setUp(self):
        self.schema_file = 'schemas/pheno_schema.json'
        with open(self.schema_file, 'r') as f:
            self.schema = json.load(f)
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
            "Phenotype": "Anemia"
            # Missing "Measurement"
        }
        self.df = pd.DataFrame([self.valid_data, self.invalid_data])

    def test_validate_schema_valid(self):
        is_valid, error = validate_schema(self.valid_data, self.schema)
        self.assertTrue(is_valid)
        self.assertIsNone(error)

    def test_validate_schema_invalid(self):
        is_valid, error = validate_schema(self.invalid_data, self.schema)
        self.assertFalse(is_valid)
        self.assertIsNotNone(error)

    def test_check_required_fields(self):
        required_fields = ["SampleID", "Age", "Gender", "Phenotype"]
        missing = check_required_fields(self.df, required_fields)
        self.assertEqual(len(missing), 0)

        required_fields.append("Measurement")
        missing = check_required_fields(self.df, required_fields)
        self.assertIn("Measurement", missing)

    def test_check_data_types(self):
        expected_types = {
            "SampleID": object,
            "Age": float,
            "Gender": object,
            "Phenotype": object,
            "Measurement": float
        }
        mismatches = check_data_types(self.df, expected_types)
        self.assertEqual(len(mismatches), 0)

    def test_perform_consistency_checks(self):
        inconsistencies = perform_consistency_checks(self.df)
        self.assertIn("Negative values found in column 'Age'.", inconsistencies)

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()