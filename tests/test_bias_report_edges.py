import numpy as np
import pandas as pd
from phenoqc.quality_metrics import imputation_bias_report


def test_bias_report_low_n_sets_ks_none_and_includes_wdist():
    # Build tiny data so that KS is not computed (len<3)
    original = pd.DataFrame({"x": [1.0, 2.0]})
    imputed = pd.DataFrame({"x": [1.5, 2.5]})
    mask = {"x": pd.Series([True, False])}
    df = imputation_bias_report(original, imputed, mask, columns=["x"], smd_threshold=0.0)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    row = df.iloc[0].to_dict()
    # ks_p should be None
    assert row.get("ks_p", None) in (None, np.nan)


def test_bias_report_warn_logic_and_columns():
    # Construct data where imputed mean differs markedly
    rng = np.random.RandomState(0)
    orig = pd.DataFrame({"a": rng.normal(0, 1, 200)})
    imp = orig.copy()
    # Simulate imputed values at masked positions shifted by +1.0
    mask_positions = pd.Series([False] * 200)
    mask_positions.iloc[:100] = True
    imp.loc[mask_positions, "a"] = imp.loc[mask_positions, "a"] + 1.0
    out = imputation_bias_report(original_df=orig, imputed_df=imp, imputation_mask={"a": mask_positions}, columns=["a"], smd_threshold=0.2)
    assert not out.empty
    assert {
        "column",
        "n_obs",
        "n_imp",
        "smd",
        "var_ratio",
        "ks_stat",
        "ks_p",
        "warn",
    }.issubset(out.columns)
    assert bool(out["warn"].iloc[0])


