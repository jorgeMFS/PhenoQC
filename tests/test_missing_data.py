import unittest
import logging
import pandas as pd
from src.missing_data import detect_missing_data, impute_missing_data, flag_missing_data_records
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
         
class TestMissingDataModule(unittest.TestCase):
    def setUp(self):
        # Sample DataFrame with missing values
        self.df = pd.DataFrame({
            "SampleID": ["S001", "S002", "S003", "S004", "S005"],
            "Age": [34, None, 45, 30, None],
            "Gender": ["Male", "Female", None, "Male", "Female"],
            "Phenotype": ["Hypertension", "Diabetes", "Asthma", "Hypertension", "Diabetes"],
            "Measurement": [120, 85, 95, None, 110]
        })

    def test_detect_missing_data(self):
        missing = detect_missing_data(self.df)
        expected_missing = pd.Series({"Age": 2, "Gender": 1, "Measurement": 1})
        pd.testing.assert_series_equal(missing, expected_missing)

    def test_flag_missing_data_records(self):
        df_flagged = flag_missing_data_records(self.df)
        expected_flags = pd.Series([False, True, True, True, True], name='MissingDataFlag')
        pd.testing.assert_series_equal(df_flagged['MissingDataFlag'], expected_flags)

    def test_impute_missing_data_mean(self):
        """Test imputing missing data using the 'mean' strategy."""
        imputed_df = impute_missing_data(self.df.copy(), strategy='mean')
        expected_age = self.df["Age"].mean()
        expected_measurement = self.df["Measurement"].mean()
        self.assertAlmostEqual(imputed_df.at[1, "Age"], expected_age)
        self.assertAlmostEqual(imputed_df.at[4, "Age"], expected_age)
        self.assertAlmostEqual(imputed_df.at[3, "Measurement"], expected_measurement)
        # Non-numeric column 'Gender' should remain unchanged
        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]))

    def test_impute_missing_data_median(self):
        """Test imputing missing data using the 'median' strategy."""
        imputed_df = impute_missing_data(self.df.copy(), strategy='median')
        expected_age = self.df["Age"].median()
        expected_measurement = self.df["Measurement"].median()
        self.assertAlmostEqual(imputed_df.at[1, "Age"], expected_age)
        self.assertAlmostEqual(imputed_df.at[4, "Age"], expected_age)
        self.assertAlmostEqual(imputed_df.at[3, "Measurement"], expected_measurement)
        # Non-numeric column 'Gender' should remain unchanged
        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]))

    def test_impute_missing_data_mode(self):
        """Test imputing missing data using the 'mode' strategy."""
        imputed_df = impute_missing_data(self.df.copy(), strategy='mode')
        expected_gender_mode = self.df["Gender"].mode()[0]  # 'Female' is the mode
        self.assertEqual(imputed_df.at[2, "Gender"], expected_gender_mode)
        # Ensure numeric columns are untouched by 'mode'
        self.assertTrue(pd.isnull(imputed_df.at[1, "Age"]))
        self.assertTrue(pd.isnull(imputed_df.at[4, "Age"]))
        self.assertTrue(pd.isnull(imputed_df.at[3, "Measurement"]))

    def test_impute_missing_data_knn(self):
        """Test imputing missing data using the 'knn' strategy."""
        imputed_df = impute_missing_data(self.df.copy(), strategy='knn')
        # Check that numeric missing values are filled
        self.assertFalse(imputed_df["Age"].isnull().any(), "Not all missing 'Age' values were imputed.")
        self.assertFalse(imputed_df["Measurement"].isnull().any(), "Not all missing 'Measurement' values were imputed.")
        # Non-numeric column 'Gender' should remain unchanged
        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]))

    def test_impute_missing_data_mice(self):
        """Test imputing missing data using the 'mice' strategy."""
        imputed_df = impute_missing_data(self.df.copy(), strategy='mice')
        # Check that numeric missing values are filled
        self.assertFalse(imputed_df["Age"].isnull().any(), "Not all missing 'Age' values were imputed.")
        self.assertFalse(imputed_df["Measurement"].isnull().any(), "Not all missing 'Measurement' values were imputed.")
        # Non-numeric column 'Gender' should remain unchanged
        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]))

    def test_impute_missing_data_svd(self):
        """Test imputing missing data using the 'svd' strategy."""
        imputed_df = impute_missing_data(self.df.copy(), strategy='svd')
        # Check that numeric missing values are filled
        self.assertFalse(imputed_df["Age"].isnull().any(), "Not all missing 'Age' values were imputed with 'svd'.")
        self.assertFalse(imputed_df["Measurement"].isnull().any(), "Not all missing 'Measurement' values were imputed with 'svd'.")
        # Non-numeric column 'Gender' should remain unchanged
        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]))

    def test_impute_missing_data_none(self):
        """Test imputing missing data using the 'none' strategy."""
        imputed_df = impute_missing_data(self.df.copy(), strategy='none')
        # Check that missing values remain
        self.assertTrue(imputed_df["Age"].isnull().sum() == 2, "Imputation was incorrectly performed with 'none' strategy.")
        self.assertTrue(imputed_df["Measurement"].isnull().sum() == 1, "Imputation was incorrectly performed with 'none' strategy.")
        # Check that 'MissingDataFlag' is not added
        self.assertNotIn('MissingDataFlag', imputed_df.columns)

    def test_impute_missing_data_invalid_strategy(self):
        """Test handling of an invalid imputation strategy."""
        with self.assertLogs(level='WARNING') as log:
            imputed_df = impute_missing_data(self.df.copy(), strategy='invalid_strategy')
            # Assert that all three columns logged warnings
            expected_messages = [
                "Unknown imputation strategy 'invalid_strategy' for column 'Age'. Skipping imputation.",
                "Unknown imputation strategy 'invalid_strategy' for column 'Gender'. Skipping imputation.",
                "Unknown imputation strategy 'invalid_strategy' for column 'Measurement'. Skipping imputation."
            ]
            for message in expected_messages:
                self.assertTrue(
                    any(message in log_message for log_message in log.output),
                    f"Expected log message '{message}' not found."
                )
        # Ensure no imputation was performed
        self.assertTrue(imputed_df["Age"].isnull().sum() == 2, "Imputation was incorrectly performed with invalid strategy.")
        self.assertTrue(imputed_df["Measurement"].isnull().sum() == 1, "Imputation was incorrectly performed with invalid strategy.")
        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]), "Imputation was incorrectly performed with invalid strategy.")

    def test_impute_missing_data_partial_field_strategies(self):
        """Test imputing missing data with some invalid column strategies."""
        field_strategies = {
            "Age": "median",
            "Gender": "unknown_strategy",  # Invalid strategy
            "Measurement": "mean"
        }
        with self.assertLogs(level='WARNING') as log:
            imputed_df = impute_missing_data(self.df.copy(), strategy='mean', field_strategies=field_strategies)
            expected_message = "Unknown imputation strategy 'unknown_strategy' for column 'Gender'. Skipping imputation."
            self.assertTrue(
                any(expected_message in log_message for log_message in log.output),
                f"Expected log message '{expected_message}' not found."
            )
        expected_age_median = self.df["Age"].median()
        expected_measurement_mean = self.df["Measurement"].mean()
        self.assertAlmostEqual(imputed_df.at[1, "Age"], expected_age_median)
        self.assertAlmostEqual(imputed_df.at[4, "Age"], expected_age_median)
        self.assertAlmostEqual(imputed_df.at[3, "Measurement"], expected_measurement_mean)
        # Gender should remain unchanged due to invalid strategy
        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]))

    def test_impute_missing_data_all_strategies(self):
        """Comprehensive test covering all imputation strategies."""
        strategies = ['mean', 'median', 'mode', 'knn', 'mice', 'svd', 'none']
        for strategy in strategies:
            with self.subTest(strategy=strategy):
                imputed_df = impute_missing_data(self.df.copy(), strategy=strategy)
                if strategy == 'none':
                    # Check that missing values remain
                    self.assertTrue(imputed_df["Age"].isnull().sum() == 2, "Imputation was incorrectly performed with 'none' strategy.")
                    self.assertTrue(imputed_df["Measurement"].isnull().sum() == 1, "Imputation was incorrectly performed with 'none' strategy.")
                    # Check that 'MissingDataFlag' is not added
                    self.assertNotIn('MissingDataFlag', imputed_df.columns)
                elif strategy == 'mode':
                    # Check that 'Gender' is imputed
                    expected_gender_mode = self.df["Gender"].mode()[0]
                    self.assertEqual(imputed_df.at[2, "Gender"], expected_gender_mode)
                    # Check that numeric columns remain missing
                    self.assertTrue(pd.isnull(imputed_df.at[1, "Age"]), f"'Age' column should remain missing with 'mode' strategy.")
                    self.assertTrue(pd.isnull(imputed_df.at[4, "Age"]), f"'Age' column should remain missing with 'mode' strategy.")
                    self.assertTrue(pd.isnull(imputed_df.at[3, "Measurement"]), f"'Measurement' column should remain missing with 'mode' strategy.")
                else:
                    # For other strategies, check that numeric missing values are filled
                    self.assertFalse(imputed_df["Age"].isnull().any(), f"'Age' column not fully imputed with strategy '{strategy}'.")
                    self.assertFalse(imputed_df["Measurement"].isnull().any(), f"'Measurement' column not fully imputed with strategy '{strategy}'.")
                    # Non-numeric column 'Gender' should remain unchanged unless strategy is 'mode'
                    if strategy == 'mode':
                        expected_gender_mode = self.df["Gender"].mode()[0]
                        self.assertEqual(imputed_df.at[2, "Gender"], expected_gender_mode)
                    else:
                        self.assertTrue(pd.isnull(imputed_df.at[2, "Gender"]), f"'Gender' column was incorrectly imputed with strategy '{strategy}'.")

if __name__ == '__main__':
    unittest.main()
