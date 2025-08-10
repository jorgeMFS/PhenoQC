# PhenoQC

[![Tests](https://github.com/jorgeMFS/PhenoQC/actions/workflows/tests.yml/badge.svg)](https://github.com/jorgeMFS/PhenoQC/actions/workflows/tests.yml)
[![Coverage](https://codecov.io/gh/jorgeMFS/PhenoQC/branch/main/graph/badge.svg)](https://codecov.io/gh/jorgeMFS/PhenoQC)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PyPI version](https://badge.fury.io/py/phenoqc.svg)](https://badge.fury.io/py/phenoqc)
[![Downloads](https://static.pepy.tech/badge/phenoqc)](https://pepy.tech/project/phenoqc)
[![Documentation Status](https://readthedocs.org/projects/phenoqc/badge/?version=latest)](https://phenoqc.readthedocs.io/en/latest/?badge=latest)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14714540.svg)](https://doi.org/10.5281/zenodo.14714540)

**PhenoQC** is a lightweight, efficient, and user-friendly toolkit designed to perform comprehensive quality control (QC) on phenotypic datasets. It ensures that data adheres to standardized formats, maintains consistency, and is harmonized with recognized ontologies—facilitating seamless integration with genomic data for advanced research.

---

## Key Features

- **Comprehensive Data Validation:**  
  Checks format compliance, schema adherence, and data consistency against JSON schemas.

- **Ontology Mapping:**  
  Maps phenotypic terms to standardized ontologies (HPO, DO, MPO) with synonym resolution and optional custom mappings.

- **Missing Data Handling:**  
  Detects and optionally imputes missing data (e.g., mean, median, mode, KNN, MICE, SVD) or flags records for manual review.

- **Batch Processing:**  
  Processes multiple files simultaneously in parallel, streamlining large-scale data QC.

- **User-Friendly Interfaces:**  
  Provides a command-line interface (CLI) for power users and a Streamlit-based GUI for interactive workflows.

- **Reporting and Visualization:**  
  Generates detailed QC reports (PDF or Markdown) and produces visual summaries of data quality metrics.

- **Extensibility:**  
  Modular design supports easy customization of validation rules, mapping expansions, or new ontologies.

- **Class Distribution (Optional):**  
  Provide a label column to get a class-imbalance summary and warning if the minority proportion falls below a threshold.

- **Imputation Parameters and Quick Tuning:**  
  Configure imputation parameters (e.g., KNN `n_neighbors`) and optionally run a mask-and-score tuner to select good parameters on observed entries.

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI](#cli)
- [GUI](#gui)
- [Reports](#reports)
- [Examples and Scripts](#examples-and-scripts)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## Installation

PhenoQC requires Python 3.9+.

### From PyPI

```bash
pip install phenoqc
```

### From source

```bash
git clone https://github.com/jorgeMFS/PhenoQC.git
cd PhenoQC
pip install -e .
```

For local development without installation you can run:

```bash
python -m phenoqc.cli
```

Dependencies are listed in `requirements.txt`.

---

## Quick Start

```bash
phenoqc --help

# Minimal run
phenoqc \
  --input examples/samples/sample_data.csv \
  --schema examples/schemas/pheno_schema.json \
  --config config.yaml \
  --unique_identifiers SampleID \
  --output ./reports/
```

Enable class distribution and imputation tuning:

```bash
phenoqc \
  --input data.csv \
  --schema schema.json \
  --config config.yaml \
  --unique_identifiers SampleID \
  --label-column class --imbalance-threshold 0.10 \
  --impute-params '{"n_neighbors": 5}' --impute-tuning on \
  --output ./reports/
```

---

## CLI

PhenoQC provides a flexible command-line interface suited for automation.

### Examples

#### Process a single file

```bash
phenoqc \
  --input examples/samples/sample_data.json \
  --output ./reports/ \
  --schema examples/schemas/pheno_schema.json \
  --config config.yaml \
  --custom_mappings examples/mapping/custom_mappings.json \
  --impute mice \
  --unique_identifiers SampleID \
  --phenotype_columns '{"PrimaryPhenotype": ["HPO"], "DiseaseCode": ["DO"]}' \
  --ontologies HPO DO
```

#### Batch process multiple files

```bash
phenoqc \
  --input examples/samples/sample_data.csv examples/samples/sample_data.json examples/samples/sample_data.tsv \
  --output ./reports/ \
  --schema examples/schemas/pheno_schema.json \
  --config config.yaml \
  --impute none \
  --unique_identifiers SampleID \
  --ontologies HPO DO MPO \
  --phenotype_columns '{"PrimaryPhenotype": ["HPO"], "DiseaseCode": ["DO"], "TertiaryPhenotype": ["MPO"]}'
```

### Useful flags

- `--impute-params '{"n_neighbors": 5}'` (JSON)
- `--impute-tuning on|off`
- `--label-column class` with `--imbalance-threshold 0.10`

Reports generated under `--output` include:

- Summary & scores
- Optional Class Distribution when a label column is set
- Additional Quality Dimensions when computed
- Missing data summary, mapping success, and visuals

### Key parameters

- `--input`: one or more data files or directories (`.csv`, `.tsv`, `.json`, `.zip`)
- `--output`: directory for saved reports (default `./reports/`)
- `--schema`: JSON schema for validation
- `--config`: YAML configuration (default `config.yaml`)
- `--custom_mappings`: custom term-mapping JSON (optional)
- `--impute`: strategy for missing data (`mean`, `median`, `mode`, `knn`, `mice`, `svd`, `none`)
- `--unique_identifiers`: columns that uniquely identify records
- `--phenotype_columns`: JSON mapping of columns to ontologies
- `--ontologies`: list of ontology IDs (e.g., `HPO DO MPO`)
- `--recursive`: enable recursive directory scanning

Per-column overrides accept their own params. The quick tuner supports KNN, MICE (`max_iter`) and SVD (`rank`); you can pass a grid to explore.

---

## GUI

Launch the Streamlit interface:

```bash
python run_gui.py
```

Workflow:

1. Upload config & schema.
2. Upload data files or a `.zip` archive.
3. Choose unique identifiers & ontologies.
4. Set missing data strategy and optional tuning.
5. Review results and download reports.

The GUI also surfaces class-distribution warnings and imputation summaries.

---

## Reports

- Class Distribution: table and warning when minority proportion < threshold
- Imputation Settings: global strategy/params and tuning summary
- Additional Quality: only displayed if metrics are computed

---

## Examples and Scripts

- `scripts/e2e_small_quality_metrics_cli_test.py` – small demo focusing on quality metrics
- `scripts/e2e_medium_cli_test.py` – mid-sized end-to-end pipeline run
- `scripts/end_to_end_e2e_cli_test.py` – large end-to-end pipeline run
- `scripts/imputation_params_cli_test.py` – imputation params and optional tuning

---

## Configuration

PhenoQC relies on a YAML config file (e.g., `config.yaml`) to define ontologies, fuzzy matching thresholds, caching, and imputation defaults.

**Sample `config.yaml`:**

```yaml
ontologies:
  HPO:
    name: Human Phenotype Ontology
    source: url
    url: http://purl.obolibrary.org/obo/hp.obo
    format: obo
  DO:
    name: Disease Ontology
    source: url
    url: http://purl.obolibrary.org/obo/doid.obo
    format: obo
  MPO:
    name: Mammalian Phenotype Ontology
    source: url
    url: http://purl.obolibrary.org/obo/mp.obo
    format: obo

default_ontologies:
  - HPO
  - DO
  - MPO

fuzzy_threshold: 80
cache_expiry_days: 30

imputation:
  strategy: knn
  params:
    n_neighbors: 5
    weights: uniform
  per_column:
    Creatinine_mgdl:
      strategy: mice
      params:
        max_iter: 15
    Cholesterol_mgdl:
      strategy: svd
      params:
        rank: 3
  tuning:
    enable: true
    mask_fraction: 0.1
    scoring: MAE
    max_cells: 20000
    random_state: 42
    grid:
      n_neighbors: [3, 5, 7]
```

Note: Labels are never modified and are excluded from the imputation matrix when a `label_column` is provided.

## Troubleshooting

- **Ontology Mapping Failures**: Check if `config.yaml` points to valid ontology URLs or local files.  
- **Missing Required Columns**: Ensure columns specified as unique identifiers or phenotypic columns exist in the dataset.  
- **Imputation Errors**: Some strategies (e.g., `mean`) only apply to numeric columns.  
- **Logs**: Consult the `phenoqc_*.log` file for in-depth error messages.  

---

## Contributing

1. **Fork** the repository on [GitHub](https://github.com/jorgeMFS/PhenoQC).  
2. **Create a branch**, implement changes, and add tests or documentation as appropriate.  
3. **Open a Pull Request** describing your contribution.  

We welcome improvements that enhance PhenoQC's functionality or documentation.

---

## License

Distributed under the [MIT License](LICENSE).

---

## Contact

**Maintainer**:  
Jorge Miguel Ferreira da Silva  
jorge(dot)miguel(dot)ferreira(dot)silva(at)ua(dot)pt

For more details, see the [GitHub Wiki](https://github.com/jorgeMFS/PhenoQC/wiki) or open an issue on [GitHub](https://github.com/jorgeMFS/PhenoQC/issues).

---

*Last updated: August 10, 2025.*
