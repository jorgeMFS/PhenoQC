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