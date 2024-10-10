import unittest
import os
import pandas as pd
import tempfile
from src.reporting import generate_qc_report, create_visual_summary

class TestReportingModule(unittest.TestCase):
    def setUp(self):
        self.validation_results = {
            "SampleID": "Valid",
            "Age": "Valid",
            "Gender": "Valid",
            "Phenotype": "Valid",
            "Measurement": "Valid"
        }
        self.missing_data = pd.Series({"Age": 1, "Measurement": 1})
        self.output_report = tempfile.mktemp(suffix='.pdf')
        self.output_image = tempfile.mktemp(suffix='.png')

    def tearDown(self):
        if os.path.exists(self.output_report):
            os.remove(self.output_report)
        if os.path.exists(self.output_image):
            os.remove(self.output_image)

    def test_generate_qc_report(self):
        generate_qc_report(self.validation_results, self.missing_data, self.output_report)
        self.assertTrue(os.path.exists(self.output_report))

    def test_create_visual_summary(self):
        create_visual_summary(self.missing_data, self.output_image)
        self.assertTrue(os.path.exists(self.output_image))

    def test_create_visual_summary_no_missing_data(self):
        empty_missing = pd.Series()
        create_visual_summary(empty_missing, "empty_missing_data.png")
        self.assertFalse(os.path.exists("empty_missing_data.png"))  # Should not create file

if __name__ == '__main__':
    unittest.main()