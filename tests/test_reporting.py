import unittest
import os
import pandas as pd
import tempfile
from src.reporting import generate_qc_report, create_visual_summary
from PIL import Image  # Import PIL for creating valid images

class TestReportingModule(unittest.TestCase):
    def setUp(self):
        # Sample validation results
        self.validation_results = {
            "Format Validation": True,
            "Duplicate Records": pd.DataFrame(),
            "Conflicting Records": pd.DataFrame(),
            "Integrity Issues": pd.DataFrame()
        }
        # Sample missing data as Series
        self.missing_data = pd.Series({
            "Age": 1,
            "Measurement": 2
        })
        self.flagged_records_count = 2
        # Sample mapping success rates
        self.mapping_success_rates = {
            "HPO": {
                "total_terms": 100,
                "mapped_terms": 90,
                "success_rate": 90.0
            },
            "DO": {
                "total_terms": 80,
                "mapped_terms": 70,
                "success_rate": 87.5
            }
        }
        # Sample visualization images (using temporary files)
        self.visualization_images = []
        self.temp_image1 = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
        self.temp_image2 = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
        self._create_dummy_image(self.temp_image1.name, color='red')
        self._create_dummy_image(self.temp_image2.name, color='blue')
        self.visualization_images.extend([self.temp_image1.name, self.temp_image2.name])
        # Paths for the output report
        self.output_report_pdf = tempfile.mktemp(suffix='.pdf')
        self.output_report_md = tempfile.mktemp(suffix='.md')
        # Define impute_strategy for tests
        self.impute_strategy = 'mean' 

    def tearDown(self):
        # Clean up the created files after tests
        for file_path in [
            self.output_report_pdf, 
            self.output_report_md, 
            self.temp_image1.name, 
            self.temp_image2.name
        ]:
            if os.path.exists(file_path):
                os.remove(file_path)

    def _create_dummy_image(self, path, color='white'):
        """Creates a simple 10x10 PNG image and saves it to the specified path."""
        img = Image.new('RGB', (10, 10), color=color)
        img.save(path, 'PNG')

    def test_generate_qc_report_pdf(self):
        """Test generating a PDF QC report."""
        generate_qc_report(
            self.validation_results,
            self.missing_data,
            self.flagged_records_count,
            self.mapping_success_rates,
            self.visualization_images,
            self.impute_strategy,  # Added impute_strategy
            self.output_report_pdf,
            report_format='pdf'
        )
        self.assertTrue(os.path.exists(self.output_report_pdf), "PDF QC report was not created.")
        # Optionally, add more assertions to verify PDF content

    def test_generate_qc_report_md(self):
        """Test generating a Markdown QC report."""
        generate_qc_report(
            self.validation_results,
            self.missing_data,
            self.flagged_records_count,
            self.mapping_success_rates,
            self.visualization_images,
            self.impute_strategy,  # Added impute_strategy
            self.output_report_md,
            report_format='md'
        )
        self.assertTrue(os.path.exists(self.output_report_md), "Markdown QC report was not created.")
        # Optionally, add more assertions to verify Markdown content

    def test_create_visual_summary_with_missing_data(self):
        """Test creating visual summaries with missing data."""
        # Convert Series to DataFrame with meaningful column names
        missing_data_df = self.missing_data.to_frame(name='MissingData')
        figs = create_visual_summary(missing_data_df, output_image_path=None)
        self.assertIsInstance(figs, list, "Visual summaries should return a list of figures.")
        self.assertGreater(len(figs), 0, "There should be at least one visual summary created.")

    def test_create_visual_summary_no_missing_data(self):
        """Test creating visual summaries with no missing data."""
        # Create a DataFrame with no missing data
        no_missing_data = pd.DataFrame({
            "Age": [0],
            "Measurement": [0]
        })
        figs = create_visual_summary(no_missing_data, output_image_path=None)
        self.assertIsInstance(figs, list, "Visual summaries should return a list of figures.")
        self.assertGreater(len(figs), 0, "There should be at least one visual summary created.")

    def test_create_visual_summary_invalid_input(self):
        """Test creating visual summaries with invalid input type."""
        with self.assertRaises(TypeError):
            # Passing an integer instead of DataFrame
            create_visual_summary(42, self.output_report_pdf)
        
        with self.assertRaises(TypeError):
            # Passing a list instead of DataFrame
            create_visual_summary(["Age", "Measurement"], self.output_report_pdf)
        
        with self.assertRaises(TypeError):
            # Passing a pandas Series instead of DataFrame
            create_visual_summary(self.missing_data, self.output_report_pdf)

    def test_generate_qc_report_with_no_issues_pdf(self):
        """Test generating a PDF QC report with no validation issues."""
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
            self.mapping_success_rates,
            self.visualization_images,
            self.impute_strategy,  # Added impute_strategy
            self.output_report_pdf,
            report_format='pdf'
        )
        self.assertTrue(os.path.exists(self.output_report_pdf), "PDF QC report was not created.")

    def test_generate_qc_report_with_no_issues_md(self):
        """Test generating a Markdown QC report with no validation issues."""
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
            self.mapping_success_rates,
            self.visualization_images,
            self.impute_strategy,  # Added impute_strategy
            self.output_report_md,
            report_format='md'
        )
        self.assertTrue(os.path.exists(self.output_report_md), "Markdown QC report was not created.")

if __name__ == '__main__':
    unittest.main()
