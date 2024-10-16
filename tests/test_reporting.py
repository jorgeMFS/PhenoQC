import unittest
import os
import pandas as pd
import tempfile
from src.reporting import generate_qc_report, create_visual_summary

class TestReportingModule(unittest.TestCase):
    def setUp(self):
        self.validation_results = {
            "Format Validation": True,
            "Duplicate Records": pd.DataFrame(),
            "Conflicting Records": pd.DataFrame(),
            "Integrity Issues": pd.DataFrame()
        }
        self.missing_data = pd.Series({"Age": 1, "Measurement": 1})
        self.flagged_records_count = 2
        self.output_report = tempfile.mktemp(suffix='.pdf')
        self.output_image = tempfile.mktemp(suffix='.png')

    def tearDown(self):
        if os.path.exists(self.output_report):
            os.remove(self.output_report)
        if os.path.exists(self.output_image):
            os.remove(self.output_image)

    def test_generate_qc_report(self):
        generate_qc_report(self.validation_results, self.missing_data, self.flagged_records_count, self.output_report)
        self.assertTrue(os.path.exists(self.output_report))

    def test_create_visual_summary(self):
        create_visual_summary(self.missing_data, self.output_image)
        self.assertTrue(os.path.exists(self.output_image))

    def test_create_visual_summary_no_missing_data(self):
        empty_missing = pd.Series(dtype=int)
        create_visual_summary(empty_missing, self.output_image)
        self.assertFalse(os.path.exists(self.output_image))  # Should not create file

if __name__ == '__main__':
    unittest.main()
