import unittest
import pandas as pd
from src.validation import DataValidator

class TestValidationModule(unittest.TestCase):
    def setUp(self):
        # Sample DataFrame
        self.data = {
            "SampleID": ["S001", "S002", "S003", "S004"],
            "Age": [34, 28, 45, 30],
            "Gender": ["Male", "Female", "Other", "Male"],
            "Phenotype": ["Hypertension", "Diabetes", "Asthma", "Hypertension"],
            "Measurement": [120, 85, 95, None]
        }
        self.df = pd.DataFrame(self.data)

        # Sample JSON schema
        self.schema = {
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
        }

        # Unique identifiers
        self.unique_identifiers = ['SampleID']

        # Initialize DataValidator
        self.validator = DataValidator(self.df, self.schema, self.unique_identifiers)

    def test_validate_format(self):
        """Test format validation with valid data."""
        result = self.validator.validate_format()
        self.assertTrue(result)

    def test_identify_duplicates(self):
        """Test duplicate identification."""
        # No duplicates in the initial sample data
        duplicates = self.validator.identify_duplicates()
        self.assertTrue(duplicates.empty)

        # Add a duplicate
        new_record = {"SampleID": "S001", "Age": 34, "Gender": "Male", "Phenotype": "Hypertension", "Measurement": 120}
        self.df = pd.concat([self.df, pd.DataFrame([new_record])], ignore_index=True)
        self.validator = DataValidator(self.df, self.schema, self.unique_identifiers)
        duplicates = self.validator.identify_duplicates()
        self.assertFalse(duplicates.empty)
        self.assertEqual(len(duplicates), 2)

    def test_detect_conflicts(self):
        """Test conflict detection among duplicates."""
        # No duplicates initially, so no conflicts
        conflicts = self.validator.detect_conflicts()
        self.assertTrue(conflicts.empty)

        # Add a conflicting duplicate
        new_record = {"SampleID": "S001", "Age": 35, "Gender": "Male", "Phenotype": "Hypertension", "Measurement": 125}
        self.df = pd.concat([self.df, pd.DataFrame([new_record])], ignore_index=True)
        self.validator = DataValidator(self.df, self.schema, self.unique_identifiers)
        conflicts = self.validator.detect_conflicts()
        self.assertFalse(conflicts.empty)
        self.assertEqual(len(conflicts), 2)
        self.assertIn(35, conflicts['Age'].values)

    def test_verify_integrity(self):
        """Test integrity verification."""
        # Initially, there should be no integrity issues
        integrity_issues = self.validator.verify_integrity()
        self.assertTrue(integrity_issues.empty)

        # Add record with missing required field 'Gender'
        new_record = {"SampleID": "S005", "Age": 40, "Gender": None, "Phenotype": "Asthma", "Measurement": 100}
        self.df = pd.concat([self.df, pd.DataFrame([new_record])], ignore_index=True)
        self.validator = DataValidator(self.df, self.schema, self.unique_identifiers)
        integrity_issues = self.validator.verify_integrity()
        self.assertFalse(integrity_issues.empty)
        # Check if the missing 'Gender' record is flagged
        self.assertIn("Gender", integrity_issues.columns)
        self.assertTrue(integrity_issues['Gender'].isnull().any())

        # Add record with wrong data type for 'Age'
        invalid_record = {"SampleID": "S006", "Age": "Thirty", "Gender": "Female", "Phenotype": "Diabetes", "Measurement": 90}
        self.df = pd.concat([self.df, pd.DataFrame([invalid_record])], ignore_index=True)
        self.validator = DataValidator(self.df, self.schema, self.unique_identifiers)
        integrity_issues = self.validator.verify_integrity()
        self.assertFalse(integrity_issues.empty)
        # Check if the invalid 'Age' record is flagged
        self.assertIn("Age", integrity_issues.columns)
        self.assertTrue(integrity_issues['Age'].isin(["Thirty"]).any())

    def test_run_all_validations(self):
        """Test running all validations with valid data."""
        results = self.validator.run_all_validations()
        self.assertTrue(results["Format Validation"])
        self.assertTrue(results["Duplicate Records"].empty)
        self.assertTrue(results["Conflicting Records"].empty)
        self.assertTrue(results["Integrity Issues"].empty)

    def test_run_all_validations_with_errors(self):
        """Test running all validations with introduced errors."""
        # Introduce duplicates, conflicts, and integrity issues
        duplicate_record = {"SampleID": "S001", "Age": 34, "Gender": "Male", "Phenotype": "Hypertension", "Measurement": 120}
        invalid_age_record = {"SampleID": "S002", "Age": -5, "Gender": "Female", "Phenotype": "Diabetes", "Measurement": 85}
        unknown_gender_record = {"SampleID": "S007", "Age": 30, "Gender": "Unknown", "Phenotype": "Asthma", "Measurement": 90}

        # Append the records
        self.df = pd.concat([self.df, pd.DataFrame([duplicate_record, invalid_age_record, unknown_gender_record])], ignore_index=True)
        self.validator = DataValidator(self.df, self.schema, self.unique_identifiers)
        results = self.validator.run_all_validations()

        # Assertions
        self.assertFalse(results["Format Validation"])  # Due to 'Age': -5 and 'Gender': 'Unknown'
        self.assertFalse(results["Duplicate Records"].empty)  # Duplicate 'S001'
        self.assertFalse(results["Conflicting Records"].empty)  # Conflicting 'Age'
        self.assertFalse(results["Integrity Issues"].empty)  # 'Age': -5 and 'Gender': 'Unknown'

        # Additional Checks
        # Check specific integrity issues
        integrity_df = results["Integrity Issues"]
        # Check for 'Age' < 0
        self.assertIn(-5, integrity_df['Age'].values)
        # Check for 'Gender' being 'Unknown'
        self.assertIn("Unknown", integrity_df['Gender'].values)

if __name__ == '__main__':
    unittest.main()