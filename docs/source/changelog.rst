Changelog
==========================

Version 1.1.0 (2025-01-13)
----------------------------------------------------

Highlights:

* Optional Class Distribution (imbalance) summary
  * CLI: ``--label-column``, ``--imbalance-threshold``
  * GUI: label column selector and threshold input
  * PDF/MD report: Class Distribution section with imbalance warning
* Config-driven imputation parameters with optional quick tuning (mask-and-score)
  * CLI: ``--impute-params``, ``--impute-tuning``
  * GUI: strategy parameters and tuning controls; persisted in config
  * Engine: parameter passthrough for KNN/MICE/SVD; label column excluded from numeric matrix
  * PDF/MD: Imputation Settings and Tuning Summary sections
* Reporting: improved layout, table formatting, spacing, and conditional sections
* Quality Metrics: redundancy deduplication (prefer identical over correlation)
* Tests: new unit tests for class distribution, imputation params, and tuning

Breaking changes: None

Upgrade notes:

* Reinstall to refresh console entry points: ``pip uninstall -y phenoqc && pip install -e .``
* If running without install, invoke as a module: ``python -m phenoqc`` (ensure ``PYTHONPATH=src`` during development)

Version 1.0.0 (2024-01-21)
----------------------------------------------------

Initial release of PhenoQC with the following features:

* Comprehensive data validation with JSON schema support
* Ontology mapping (HPO, DO, MPO) with fuzzy matching
* Missing data detection and imputation
* Batch processing capabilities
* Command-line interface
* Streamlit-based GUI
* Detailed reporting and visualization
* Extensive test coverage
* Documentation 