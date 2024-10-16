import unittest
import os
import pandas as pd
import tempfile
from src.reporting import generate_qc_report, create_visual_summary

class TestReportingModule(unittest.TestCase):
    def setUp(self):
        # Sample validation results
        self.validation_results = {
            "Format Validation": True,
            "Duplicate Records": pd.DataFrame(),
            "Conflicting Records": pd.DataFrame(),
            "Integrity Issues": pd.DataFrame()
        }
        # Sample missing data as DataFrame
        self.missing_data = pd.DataFrame({
            "Age": [1, None, 3],
            "Measurement": [None, 2, 3]
        })
        self.flagged_records_count = 2
        self.output_report = tempfile.mktemp(suffix='.pdf')
        self.output_image = tempfile.mktemp(suffix='.html')  # Changed to .html as per create_visual_summary

    def tearDown(self):
        # Clean up the created files after tests
        if os.path.exists(self.output_report):
            os.remove(self.output_report)
        if os.path.exists(self.output_image):
            os.remove(self.output_image)

    def test_generate_qc_report(self):
        """Test generating a QC report."""
        generate_qc_report(
            self.validation_results,
            self.missing_data.sum(),  # Pass summary as Series
            self.flagged_records_count,
            self.output_report
        )
        self.assertTrue(os.path.exists(self.output_report), "QC report was not created.")

    def test_create_visual_summary_with_missing_data(self):
        """Test creating visual summaries with missing data."""
        create_visual_summary(self.missing_data, self.output_image)
        self.assertTrue(os.path.exists(self.output_image), "Visual summary was not created.")

    def test_create_visual_summary_no_missing_data(self):
        """Test creating visual summaries with no missing data."""
        # Create a DataFrame with no missing data
        no_missing_data = pd.DataFrame({
            "Age": [1, 2, 3],
            "Measurement": [4, 5, 6]
        })
        create_visual_summary(no_missing_data, self.output_image)
        self.assertTrue(os.path.exists(self.output_image), "Visual summary was not created even though there is no missing data.")

    def test_create_visual_summary_invalid_input(self):
        """Test creating visual summaries with invalid input type."""
        with self.assertRaises(TypeError):
            create_visual_summary(self.missing_data['Age'], self.output_image)  # Passing a Series instead of DataFrame

    def test_generate_qc_report_with_no_issues(self):
        """Test generating a QC report with no validation issues."""
        empty_validation = {
            "Format Validation": True,
            "Duplicate Records": pd.DataFrame(),
            "Conflicting Records": pd.DataFrame(),
            "Integrity Issues": pd.DataFrame()
        }
        no_missing_data = pd.Series({"Age": 0, "Measurement": 0})
        flagged_records = 0
        generate_qc_report(
            empty_validation,
            no_missing_data,
            flagged_records,
            self.output_report
        )
        self.assertTrue(os.path.exists(self.output_report), "QC report was not created.")

if __name__ == '__main__':
    unittest.main()
