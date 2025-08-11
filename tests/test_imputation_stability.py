import unittest
import numpy as np
import pandas as pd

from phenoqc.quality_metrics import imputation_stability_cv


class TestImputationStability(unittest.TestCase):
    def setUp(self):
        rng = np.random.RandomState(1)
        n = 500
        # Simple Gaussian columns
        self.df = pd.DataFrame({
            'g1': rng.normal(0, 1.0, n),
            'g2': rng.normal(5, 2.0, n),
        })

    def test_stability_converges_with_repeats(self):
        # With more repeats, the SD of MAE across repeats should be non-increasing on average
        res_low = imputation_stability_cv(self.df, strategy='mean', repeats=3, mask_fraction=0.1, scoring='MAE', random_state=7)
        res_high = imputation_stability_cv(self.df, strategy='mean', repeats=10, mask_fraction=0.1, scoring='MAE', random_state=7)

        self.assertIsInstance(res_low, pd.DataFrame)
        self.assertIsInstance(res_high, pd.DataFrame)
        self.assertFalse(res_low.empty)
        self.assertFalse(res_high.empty)

        # Compare average sd_error
        avg_sd_low = res_low['sd_error'].mean()
        avg_sd_high = res_high['sd_error'].mean()
        self.assertLessEqual(avg_sd_high, avg_sd_low + 1e-8)


if __name__ == '__main__':
    unittest.main()


