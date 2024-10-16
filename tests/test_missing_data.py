import unittest
import pandas as pd
from src.missing_data import detect_missing_data, impute_missing_data, flag_missing_data_records

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

    def test_flag_missing_data_records(self):
        df_flagged = flag_missing_data_records(self.df)
        expected_flags = pd.Series([False, True, True, True], name='MissingDataFlag')
        pd.testing.assert_series_equal(df_flagged['MissingDataFlag'], expected_flags)

    def test_impute_missing_data_mean(self):
        imputed_df = impute_missing_data(self.df.copy(), strategy='mean')
        expected_age = (34 + 45 + 30) / 3
        expected_measurement = (120 + 85 + 95) / 3
        self.assertAlmostEqual(imputed_df.at[1, "Age"], expected_age)
        self.assertAlmostEqual(imputed_df.at[3, "Measurement"], expected_measurement)
        # Gender should remain unchanged as mean imputation is not applicable
        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]))

    def test_impute_missing_data_mode(self):
        imputed_df = impute_missing_data(self.df.copy(), strategy='mode')
        expected_gender_mode = "Male"  # Mode of ["Male", "Female", None, "Male"] is "Male"
        self.assertEqual(imputed_df.at[2, "Gender"], expected_gender_mode)
        expected_phenotype_mode = "Hypertension"
        self.assertFalse(imputed_df['Phenotype'].isnull().any())
        self.assertEqual(imputed_df.at[2, "Phenotype"], "Asthma")  # No missing, should remain unchanged

    def test_impute_missing_data_field_strategies(self):
        field_strategies = {"Age": "median", "Gender": "mode", "Measurement": "mean"}
        imputed_df = impute_missing_data(self.df.copy(), strategy='mean', field_strategies=field_strategies)
        expected_age_median = 34.0  # Median of [34,45,30]
        self.assertAlmostEqual(imputed_df.at[1, "Age"], expected_age_median)
        expected_gender_mode = "Male"
        self.assertEqual(imputed_df.at[2, "Gender"], expected_gender_mode)
        expected_measurement_mean = (120 + 85 + 95) / 3
        self.assertAlmostEqual(imputed_df.at[3, "Measurement"], expected_measurement_mean)

    def test_impute_missing_data_invalid_strategy(self):
        with self.assertLogs(level='WARNING') as log:
            imputed_df = impute_missing_data(self.df.copy(), strategy='invalid')
            self.assertIn("Unknown imputation strategy", log.output[0])

if __name__ == '__main__':
    unittest.main()
