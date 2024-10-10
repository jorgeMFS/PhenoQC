import unittest
import pandas as pd
from src.missing_data import detect_missing_data, impute_missing_data

class TestMissingDataModule(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            "SampleID": ["S001", "S002", "S003", "S004"],
            "Age": [34, None, 45, 30],
            "Gender": ["Male", "Female", None, "Male"],
            "Phenotype": ["Hypertension", "Diabetes", "Asthma", "Hypertension"],
            "Measurement": [120, 85, 95, None]
        })

    def test_detect_missing_data(self):
        missing = detect_missing_data(self.df)
        expected_missing = pd.Series({"Age": 1, "Gender": 1, "Measurement": 1})
        pd.testing.assert_series_equal(missing, expected_missing)

    def test_impute_missing_data_mean(self):
        imputed_df = impute_missing_data(self.df, strategy='mean')
        expected_age = (34 + 45 + 30) / 3
        expected_measurement = (120 + 85 + 95) / 3
        self.assertAlmostEqual(imputed_df.at[1, "Age"], expected_age)
        self.assertAlmostEqual(imputed_df.at[3, "Measurement"], expected_measurement)
        # Gender should remain NaN as it's categorical
        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]))

    def test_impute_missing_data_median(self):
        imputed_df = impute_missing_data(self.df, strategy='median')
        expected_age = 34  # Median of [34,45,30] is 34
        expected_measurement = 95  # Median of [120,85,95] is 95
        self.assertAlmostEqual(imputed_df.at[1, "Age"], expected_age)
        self.assertAlmostEqual(imputed_df.at[3, "Measurement"], expected_measurement)
        # Gender should remain NaN as it's categorical
        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]))

    def test_impute_missing_data_invalid_strategy(self):
        with self.assertRaises(ValueError):
            impute_missing_data(self.df, strategy='invalid')

if __name__ == '__main__':
    unittest.main()