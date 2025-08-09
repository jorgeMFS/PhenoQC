import pandas as pd
import pytest

from phenoqc.quality_metrics import report_class_distribution, ClassCounter


def test_class_distribution_single_pass_warning():
    # 95 A, 5 B => minority proportion 0.05 < 0.10 => warning True
    df = pd.DataFrame({
        'label': ['A'] * 95 + ['B'] * 5
    })
    res = report_class_distribution(df, label_column='label', warn_threshold=0.10)
    assert res.warning is True
    assert res.minority_class == 'B'
    assert pytest.approx(res.proportions['B'], rel=1e-6) == 0.05


def test_class_distribution_chunked_equals_single_pass():
    df1 = pd.DataFrame({'label': ['A'] * 50 + ['B'] * 2})
    df2 = pd.DataFrame({'label': ['A'] * 45 + ['B'] * 3})

    single = report_class_distribution(
        pd.concat([df1, df2], ignore_index=True),
        label_column='label',
        warn_threshold=0.10,
    )

    counter = ClassCounter()
    counter.update(df1['label'])
    counter.update(df2['label'])
    chunked = counter.finalize(warn_threshold=0.10)

    assert single.counts == chunked.counts
    assert single.warning == chunked.warning

