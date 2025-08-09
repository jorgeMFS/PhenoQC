PhenoQC v1.1.0 — 2025-01-13
===========================

Summary
-------

This release introduces optional Class Distribution (imbalance) analysis, configurable imputation parameters, and an optional quick tuning (mask-and-score) mechanism. The PDF report layout was refined with better table styling, spacing, and conditional sections. Several unit tests and scripts were added to validate the new behavior.

Highlights
---------

- Class Distribution (imbalance) summary
  - CLI: `--label-column`, `--imbalance-threshold`
  - GUI: label column selector and threshold input
  - Reports: table + warning when minority proportion < threshold
- Imputation parameters + optional quick tuning
  - CLI: `--impute-params` (JSON), `--impute-tuning on|off`
  - Config: new `imputation:` block with `strategy`, `params`, `per_column`, `tuning`
  - Engine: parameter passthrough for KNN/MICE/SVD; pluggable tuner; label column excluded from imputation matrix
  - Reports: Imputation Settings and Tuning Summary sections
- Reporting improvements: landscape pages, compact margins, styled tables, white headers, divider lines, page numbers, conditional sections
- Redundancy metric: deduplication (prefer identical over correlation for the same pair)
- Tests & scripts: new unit tests and comprehensive CLI scripts for metrics and tuning

Upgrade Notes
-------------

- If previously installed, refresh the console entry point:
  - `pip uninstall -y phenoqc && pip install -e .`
- For local development without installing, prefer module invocation and set PYTHONPATH:
  - `PYTHONPATH=src python -m phenoqc --help`

Links
-----

- Changelog: `CHANGELOG.md`
- Docs: `https://phenoqc.readthedocs.io/en/latest/`