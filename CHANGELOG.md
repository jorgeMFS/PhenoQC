## [1.2.0] - 2025-08-14
- GUI: strategy-agnostic imputation panel (config-driven; per-column overrides; generic tuning)
- GUI: fix quality metrics picker defaults when config uses dict-style metrics
- Reporting: include categorical PSI/Cramér's V in bias warning header and per-row triggers
- Scripts: add clinical_all_features_e2e.py (full dataset/schema/config/custom mappings; online+offline runs)
- Fix: Python 3.9 compatibility in GUI typing; minor tuning UI improvements

## [1.1.1] - 2025-01-13
- feat: add class distribution bar chart to PDF and Markdown reports
- feat: persist per-file *_qc_summary.json with quality_scores, imputation, class_distribution
- fix: Python 3.9 typing compatibility in ImputationEngine (avoid PEP 604 unions)
- chore: docs and scripts updated to assert and document new artifact

## [1.1.0] - 2025-01-13
- feat: Optional class-distribution summary (label-aware)
  - CLI: `--label-column`, `--imbalance-threshold`
  - GUI: label column selector and threshold input
  - PDF/MD report: Class Distribution section with imbalance warning
- feat: Config-driven imputation parameters with optional quick tuning (mask-and-score)
  - CLI: `--impute-params`, `--impute-tuning`
  - GUI: strategy params and tuning controls; persisted in config
  - Engine: KNN param passthrough and tuning; label column excluded from numeric matrix
  - PDF/MD: Imputation Settings and Tuning Summary sections
- fix: Redundancy metric deduplication (prefer identical over correlation)
- test: Added unit tests for class distribution, imputation params, tuning skeleton, and config load
# Changelog

All notable changes to PhenoQC will be documented in this file.

## [0.1.0] - 2024-10-18

### Added

- Initial release of PhenoQC
- Data validation functionality:
  - Schema validation using JSON schema
  - Format compliance checks
  - Integrity verification
  - Duplicate record detection
  - Conflicting record identification
- Ontology mapping feature:
  - Support for multiple ontologies (HPO, DO, MPO)
  - Custom mapping support
  - Synonym resolution
- Missing data detection and imputation:
  - Multiple imputation strategies (mean, median, mode, KNN, MICE, SVD)
  - Option to flag records with missing data
- Batch processing capability:
  - Support for multiple file processing
  - Parallel execution
- Command-line interface (CLI)
- Streamlit-based graphical user interface (GUI)
- Reporting and visualization:
  - PDF and Markdown report generation
  - Visual summaries using Plotly
- Support for CSV, TSV, and JSON input files
- Recursive directory scanning option
- Comprehensive logging system
- YAML-based configuration for ontology mappings and imputation strategies

### Changed

- Improved error handling and logging
- Updated documentation and examples

### Fixed

- Resolved critical bugs related to data validation and ontology mapping