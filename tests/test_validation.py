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

    def test_format_validation(self):
        """
        Test overall format validation using run_all_validations() on valid data.
        We expect 'Format Validation' to be True, and 'Integrity Issues' to be empty.
        """
        results = self.validator.run_all_validations()
        self.assertTrue(results["Format Validation"], "Format validation failed for valid data.")
        self.assertTrue(results["Integrity Issues"].empty, "Unexpected integrity issues in valid data.")

    def test_identify_duplicates(self):
        """Test duplicate identification."""
        # No duplicates in the initial sample data
        duplicates = self.validator.identify_duplicates()
        self.assertTrue(duplicates.empty)

        # Add a duplicate
        new_record = {
            "SampleID": "S001", 
            "Age": 34, 
            "Gender": "Male", 
            "Phenotype": "Hypertension", 
            "Measurement": 120
        }
        # Re-create the validator with an extra row duplicated
        df_duplicated = pd.concat([self.df, pd.DataFrame([new_record])], ignore_index=True)
        validator_dup = DataValidator(df_duplicated, self.schema, self.unique_identifiers)

        duplicates = validator_dup.identify_duplicates()
        self.assertFalse(duplicates.empty)
        self.assertEqual(len(duplicates), 2)

    def test_detect_conflicts(self):
        """Test conflict detection among duplicates."""
        # No duplicates initially, so no conflicts
        conflicts = self.validator.detect_conflicts()
        self.assertTrue(conflicts.empty)

        # Add a conflicting duplicate
        new_record = {
            "SampleID": "S001", 
            "Age": 35,  # conflict with original Age=34
            "Gender": "Male", 
            "Phenotype": "Hypertension", 
            "Measurement": 125
        }
        df_conflict = pd.concat([self.df, pd.DataFrame([new_record])], ignore_index=True)
        validator_conflict = DataValidator(df_conflict, self.schema, self.unique_identifiers)

        conflicts = validator_conflict.detect_conflicts()
        self.assertFalse(conflicts.empty)
        self.assertEqual(len(conflicts), 2)
        self.assertIn(35, conflicts['Age'].values)

    def test_verify_integrity(self):
        """Test integrity verification on required fields and data types."""
        # Initially, there should be no integrity issues
        integrity_issues = self.validator.verify_integrity()
        self.assertTrue(integrity_issues.empty)

        # Add record with missing required field 'Gender'
        new_record = {
            "SampleID": "S005", 
            "Age": 40, 
            "Gender": None, 
            "Phenotype": "Asthma", 
            "Measurement": 100
        }
        df_missing_gender = pd.concat([self.df, pd.DataFrame([new_record])], ignore_index=True)
        validator_missing_gender = DataValidator(df_missing_gender, self.schema, self.unique_identifiers)

        integrity_issues = validator_missing_gender.verify_integrity()
        self.assertFalse(integrity_issues.empty)
        self.assertIn("Gender", integrity_issues.columns)
        self.assertTrue(integrity_issues['Gender'].isnull().any())

        # Add record with invalid 'Age' type (string instead of number)
        invalid_record = {
            "SampleID": "S006", 
            "Age": "Thirty", 
            "Gender": "Female", 
            "Phenotype": "Diabetes", 
            "Measurement": 90
        }
        df_invalid_age = pd.concat([df_missing_gender, pd.DataFrame([invalid_record])], ignore_index=True)
        validator_invalid_age = DataValidator(df_invalid_age, self.schema, self.unique_identifiers)
        integrity_issues = validator_invalid_age.verify_integrity()

        self.assertFalse(integrity_issues.empty)
        self.assertIn("Age", integrity_issues.columns)
        # self.assertTrue(integrity_issues['Age'].isin(["Thirty"]).any())

    def test_run_all_validations(self):
        """Test run_all_validations() with valid data."""
        results = self.validator.run_all_validations()
        self.assertTrue(results["Format Validation"])
        self.assertTrue(results["Duplicate Records"].empty)
        self.assertTrue(results["Conflicting Records"].empty)
        self.assertTrue(results["Integrity Issues"].empty)

    def test_run_all_validations_with_errors(self):
        """Test run_all_validations() with introduced errors."""
        # Introduce duplicates, conflicts, and integrity issues
        duplicate_record = {
            "SampleID": "S001", 
            "Age": 34, 
            "Gender": "Male", 
            "Phenotype": "Hypertension", 
            "Measurement": 120
        }
        invalid_age_record = {
            "SampleID": "S002", 
            "Age": -5,  # invalid due to "minimum": 0
            "Gender": "Female", 
            "Phenotype": "Diabetes", 
            "Measurement": 85
        }
        unknown_gender_record = {
            "SampleID": "S007", 
            "Age": 30, 
            "Gender": "Unknown",  # invalid enum
            "Phenotype": "Asthma", 
            "Measurement": 90
        }

        # Append the new records
        df_errors = pd.concat(
            [self.df, pd.DataFrame([duplicate_record, invalid_age_record, unknown_gender_record])], 
            ignore_index=True
        )
        validator_errors = DataValidator(df_errors, self.schema, self.unique_identifiers)
        results = validator_errors.run_all_validations()

        # Assertions
        self.assertFalse(results["Format Validation"], "Format Validation should fail due to -5 Age or Unknown Gender.")
        self.assertFalse(results["Duplicate Records"].empty, "A duplicate 'S001' was added.")
        self.assertFalse(results["Conflicting Records"].empty, "Conflicts should exist for 'S001' if different columns conflict.")
        self.assertFalse(results["Integrity Issues"].empty, "Integrity issues must be present due to invalid 'Age' < 0 and 'Gender' enum mismatch.")

        # Check specific entries
        integrity_df = results["Integrity Issues"]
        self.assertIn(-5, integrity_df['Age'].values, "Negative Age (-5) wasn't captured in integrity issues.")
        self.assertIn("Unknown", integrity_df['Gender'].values, "'Unknown' gender wasn't captured in integrity issues.")

    def test_referential_integrity_check(self):
        """Ensure referential integrity check does not raise with valid reference data."""
        ref_df = pd.DataFrame({"SampleID": ["S001", "S002", "S003", "S004"]})
        validator = DataValidator(
            self.df,
            self.schema,
            self.unique_identifiers,
            reference_data=ref_df,
            reference_columns=["SampleID"],
        )
        try:
            validator.check_referential_integrity()
        except Exception as e:
            self.fail(f"check_referential_integrity raised an exception: {e}")
        self.assertTrue(validator.referential_integrity_issues.empty)

    def test_referential_integrity_missing_values(self):
        """Rows with IDs absent from reference data should be reported."""
        ref_df = pd.DataFrame({"SampleID": ["S001", "S002", "S003"]})
        df_invalid = self.df.copy()
        df_invalid.loc[3, "SampleID"] = "S999"
        validator = DataValidator(
            df_invalid,
            self.schema,
            self.unique_identifiers,
            reference_data=ref_df,
            reference_columns=["SampleID"],
        )
        validator.check_referential_integrity()
        issues = validator.referential_integrity_issues
        self.assertEqual(issues.shape[0], 1)
        self.assertEqual(issues['SampleID'].iloc[0], "S999")

if __name__ == '__main__':
    unittest.main()
