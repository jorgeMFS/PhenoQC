import unittest
import pandas as pd
import numpy as np
from phenoqc.missing_data import ImputationEngine


class TestProtectedColumns(unittest.TestCase):
    """Test that protected columns are properly excluded from imputation and tuning."""

    def setUp(self):
        """Set up test data with protected columns."""
        # Create test data with a protected outcome column
        np.random.seed(42)
        n_samples = 100
        
        self.test_data = pd.DataFrame({
            'id': range(n_samples),
            'feature1': np.random.normal(0, 1, n_samples),
            'feature2': np.random.normal(0, 1, n_samples),
            'feature3': np.random.normal(0, 1, n_samples),
            'outcome': np.random.choice([0, 1], n_samples),  # Protected column
            'label': np.random.choice(['A', 'B', 'C'], n_samples),  # Protected column
        })
        
        # Add some missing values to features (but not to protected columns)
        self.test_data.loc[10:20, 'feature1'] = np.nan
        self.test_data.loc[30:40, 'feature2'] = np.nan
        self.test_data.loc[50:60, 'feature3'] = np.nan

    def test_protected_columns_excluded_from_imputation(self):
        """Test that protected columns are not used for imputation."""
        protected_cols = ['outcome', 'label']
        engine = ImputationEngine(
            {'strategy': 'knn', 'params': {'n_neighbors': 3}}, 
            exclude_columns=protected_cols
        )
        
        # Fit and transform
        result = engine.fit_transform(self.test_data)
        
        # Protected columns should remain unchanged
        pd.testing.assert_frame_equal(
            self.test_data[protected_cols], 
            result[protected_cols]
        )
        
        # Feature columns should be imputed
        self.assertFalse(result['feature1'].isna().any())
        self.assertFalse(result['feature2'].isna().any())
        self.assertFalse(result['feature3'].isna().any())

    def test_protected_columns_excluded_from_tuning(self):
        """Test that protected columns are not used for tuning."""
        protected_cols = ['outcome', 'label']
        tuning_config = {
            'strategy': 'knn',
            'tuning': {
                'enable': True,
                'mask_fraction': 0.1,
                'scoring': 'MAE',
                'max_cells': 1000,
                'random_state': 42,
                'grid': {'n_neighbors': [3, 5]}
            }
        }
        
        engine = ImputationEngine(tuning_config, exclude_columns=protected_cols)
        
        # Fit and transform
        result = engine.fit_transform(self.test_data)
        
        # Protected columns should remain unchanged
        pd.testing.assert_frame_equal(
            self.test_data[protected_cols], 
            result[protected_cols]
        )
        
        # Tuning should have occurred
        self.assertIsNotNone(engine.tuning_summary)
        self.assertTrue(engine.tuning_summary.get('enabled', False))

    def test_protected_columns_not_in_numeric_features(self):
        """Test that protected columns are excluded from numeric feature selection."""
        protected_cols = ['outcome', 'label']
        engine = ImputationEngine(
            {'strategy': 'mean'}, 
            exclude_columns=protected_cols
        )
        
        # Get numeric columns (should exclude protected columns)
        numeric_cols = engine._numeric_columns(self.test_data)
        
        # Protected columns should not be in numeric features
        for col in protected_cols:
            self.assertNotIn(col, numeric_cols)
        
        # Feature columns should be included
        self.assertIn('feature1', numeric_cols)
        self.assertIn('feature2', numeric_cols)
        self.assertIn('feature3', numeric_cols)

    def test_protected_columns_warning_when_configured(self):
        """Test that warning is issued when protected columns are configured for imputation."""
        protected_cols = ['outcome']
        
        # Configure the protected column for imputation
        config = {
            'strategy': 'mean',
            'per_column': {
                'outcome': {'strategy': 'mode'}  # This should trigger a warning
            }
        }
        
        # Capture warnings
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            engine = ImputationEngine(config, exclude_columns=protected_cols)
            
            # Check that warning was issued
            self.assertTrue(len(w) > 0)
            warning_found = any('Protected column' in str(warning.message) for warning in w)
            self.assertTrue(warning_found)

    def test_no_protected_columns_leakage(self):
        """Test that protected columns don't influence imputation of other columns."""
        protected_cols = ['outcome']
        
        # Create data where protected column correlates with features
        correlated_data = self.test_data.copy()
        correlated_data['feature1'] = correlated_data['outcome'] * 2 + np.random.normal(0, 0.1, len(correlated_data))
        
        # Add missing values to feature1
        correlated_data.loc[10:20, 'feature1'] = np.nan
        
        engine = ImputationEngine(
            {'strategy': 'knn', 'params': {'n_neighbors': 3}}, 
            exclude_columns=protected_cols
        )
        
        # Fit and transform
        result = engine.fit_transform(correlated_data)
        
        # The imputed values should not be perfectly correlated with outcome
        # (since outcome is excluded from the feature matrix)
        imputed_values = result.loc[10:20, 'feature1']
        original_outcomes = correlated_data.loc[10:20, 'outcome']
        
        # Calculate correlation between imputed values and outcomes
        correlation = np.corrcoef(imputed_values, original_outcomes)[0, 1]
        
        # Correlation should not be perfect (1.0) since outcome is excluded
        self.assertLess(abs(correlation), 0.99)

    def test_empty_protected_columns(self):
        """Test behavior when no protected columns are specified."""
        engine = ImputationEngine({'strategy': 'mean'}, exclude_columns=None)
        
        # Should work normally without protected columns
        result = engine.fit_transform(self.test_data)
        
        # All columns should be processed
        self.assertFalse(result.isna().any().any())

    def test_protected_columns_not_in_data(self):
        """Test behavior when protected columns don't exist in data."""
        protected_cols = ['nonexistent_column']
        engine = ImputationEngine({'strategy': 'mean'}, exclude_columns=protected_cols)
        
        # Should work normally, just excluding non-existent columns
        result = engine.fit_transform(self.test_data)
        
        # All existing columns should be processed
        self.assertFalse(result.isna().any().any())


if __name__ == '__main__':
    unittest.main()
