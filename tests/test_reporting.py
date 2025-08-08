import unittest
import os
import pandas as pd
import tempfile
from phenoqc.reporting import generate_qc_report, create_visual_summary
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

        # Define quality_scores for tests
        self.quality_scores = {
            'Schema Validation Score': 95.0,
            'Missing Data Score': 98.0,
            'Mapping Success Score': 90.0,
            'Overall Quality Score': 94.33
        }

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
        from PIL import Image
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
            self.impute_strategy,
            self.quality_scores,
            self.output_report_pdf,
            report_format='pdf'
        )
        self.assertTrue(os.path.exists(self.output_report_pdf), "PDF QC report was not created.")

    def test_generate_qc_report_md(self):
        """Test generating a Markdown QC report."""
        generate_qc_report(
            self.validation_results,
            self.missing_data,
            self.flagged_records_count,
            self.mapping_success_rates,
            self.visualization_images,
            self.impute_strategy,
            self.quality_scores,
            self.output_report_md,
            report_format='md'
        )
        self.assertTrue(os.path.exists(self.output_report_md), "Markdown QC report was not created.")

    def test_create_visual_summary_with_missing_data(self):
        """Test creating visual summaries with missing data."""
        # Provide a df with missing data
        df_with_missing = pd.DataFrame({
            "Age": [34, None, 45],
            "Measurement": [120, 85, None],
            "Phenotype": ["Hypertension", "Diabetes", "Asthma"]
        })
        # Provide a minimal phenotype_columns so we do get a bar chart
        phenotype_cols = {"Phenotype": ["HPO"]}

        figs = create_visual_summary(df_with_missing, phenotype_columns=phenotype_cols, output_image_path=None)
        self.assertIsInstance(figs, list, "Visual summaries should return a list of figures.")
        self.assertGreater(len(figs), 0, "There should be at least one visual summary created.")

    def test_create_visual_summary_no_missing_data(self):
        """Test creating visual summaries with no missing data."""
        df_no_missing = pd.DataFrame({
            "Age": [34, 28, 45],
            "Measurement": [120, 85, 95],
            "Phenotype": ["Hypertension", "Diabetes", "Asthma"]
        })
        # Again, supply phenotype_columns
        phenotype_cols = {"Phenotype": ["HPO"]}

        figs = create_visual_summary(df_no_missing, phenotype_columns=phenotype_cols, output_image_path=None)
        self.assertIsInstance(figs, list, "Visual summaries should return a list of figures.")
        self.assertGreater(len(figs), 0, "There should be at least one visual summary created.")

    def test_create_visual_summary_invalid_input(self):
        """Test creating visual summaries with invalid input type."""
        # This requires that create_visual_summary does a type check at the start.
        with self.assertRaises(TypeError):
            create_visual_summary(42, output_image_path=None)
        
        with self.assertRaises(TypeError):
            create_visual_summary(["Age", "Measurement"], output_image_path=None)
        
        with self.assertRaises(TypeError):
            create_visual_summary(pd.Series([1,2,3]), output_image_path=None)

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
            self.impute_strategy,
            self.quality_scores,
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
            self.impute_strategy,
            self.quality_scores,
            self.output_report_md,
            report_format='md'
        )
        self.assertTrue(os.path.exists(self.output_report_md), "Markdown QC report was not created.")

if __name__ == '__main__':
    unittest.main()