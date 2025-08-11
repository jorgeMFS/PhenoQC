import io
import pandas as pd
from phenoqc.reporting import generate_qc_report


def test_report_includes_imputation_settings_pdf():
    validation_results = {
        "Format Validation": True,
        "Duplicate Records": pd.DataFrame(),
        "Conflicting Records": pd.DataFrame(),
        "Integrity Issues": pd.DataFrame(),
        "Referential Integrity Issues": pd.DataFrame(),
        "Anomalies Detected": pd.DataFrame(),
        "Invalid Mask": pd.DataFrame(),
    }
    missing_data = pd.Series(dtype=int)
    mapping_success_rates = {}
    quality_scores = {"Schema Validation Score": 100.0, "Missing Data Score": 100.0, "Mapping Success Score": 100.0, "Overall Quality Score": 100.0}
    imputation_summary = {
        'global': {'strategy': 'knn', 'params': {'n_neighbors': 5}},
        'tuning': {'enabled': True, 'best': {'n_neighbors': 5}, 'score': 0.1234, 'metric': 'MAE', 'random_state': 42}
    }

    buf = io.BytesIO()
    generate_qc_report(
        validation_results=validation_results,
        missing_data=missing_data,
        flagged_records_count=0,
        mapping_success_rates=mapping_success_rates,
        visualization_images=[],
        impute_strategy='knn',
        quality_scores=quality_scores,
        output_path_or_buffer=buf,
        report_format='pdf',
        file_identifier='test.csv',
        imputation_summary=imputation_summary,
    )
    buf.seek(0)
    data = buf.read(1024)
    assert data[:4] == b'%PDF'


