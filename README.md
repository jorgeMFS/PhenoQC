# PhenoQC

[![Tests](https://github.com/jorgeMFS/PhenoQC/actions/workflows/tests.yml/badge.svg)](https://github.com/jorgeMFS/PhenoQC/actions/workflows/tests.yml)
[![Coverage](https://codecov.io/gh/jorgeMFS/PhenoQC/branch/main/graph/badge.svg)](https://codecov.io/gh/jorgeMFS/PhenoQC)
[![Python 3.6+](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

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

---

## Installation

PhenoQC requires Python 3.6+.

**Using `pip`:**

```bash
pip install phenoqc
```

Or **manually** from source:

```bash
git clone https://github.com/jorgeMFS/PhenoQC.git
cd PhenoQC
pip install -e .
```

**Dependencies** are listed in `requirements.txt` and include:

- `pandas`, `jsonschema`, `requests`, `plotly`, `reportlab`, `streamlit`,  
  `pyyaml`, `watchdog`, `kaleido`, `tqdm`, `Pillow`, `scikit-learn`,  
  `fancyimpute`, `fastjsonschema`, `pronto`, `rapidfuzz`.

---

## Usage

PhenoQC can be invoked via its **CLI** or through the **GUI**:

### 1. Command-Line Interface (CLI)

#### Example: Process a Single File

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

#### Example: Batch Process Multiple Files

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

**Key Parameters:**

- `--input`: One or more data files or directories (`.csv`, `.tsv`, `.json`, `.zip`).
- `--output`: Directory for saving processed data and reports (default: `./reports/`).
- `--schema`: Path to the JSON schema for data validation.
- `--config`: YAML config file defining ontologies and settings (default: `config.yaml`).
- `--custom_mappings`: Path to a custom term-mapping JSON (optional).
- `--impute`: Strategy for missing data (e.g., `mean`, `median`, `mode`, `knn`, `mice`, `svd`, or `none`).
- `--unique_identifiers`: Columns that uniquely identify each record (e.g., `SampleID`).
- `--phenotype_columns`: JSON mapping of columns to ontologies:  
  e.g., `{"PrimaryPhenotype": ["HPO"], "DiseaseCode": ["DO"]}`
- `--ontologies`: List of ontology IDs (e.g., `HPO DO MPO`).
- `--recursive`: Enable recursive scanning of directories.

---

### 2. Graphical User Interface (GUI)

Launch the Streamlit GUI for an interactive experience:

```bash
streamlit run src/gui.py
```

**Workflow in the GUI**:
1. **Upload Config & Schema**: Provide a JSON schema and a YAML config to define validation and ontology settings.
2. **Upload Data**: Either upload individual `.csv`/`.tsv`/`.json` files or a `.zip` archive containing multiple files.
3. **Choose Unique Identifiers & Ontologies**: Select columns to map to ontologies (HPO, DO, etc.) and specify unique identifier columns (e.g., `SampleID`).
4. **Set Missing Data Strategy**: Choose an imputation strategy (mean, median, mode, advanced).
5. **Run QC**: Process data and review results. Download generated reports.

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

imputation_strategies:
  Age: mean
  Gender: mode
  Height: median
  Phenotype: mode

advanced_imputation_methods:
  - MICE
  - KNN
  - IterativeSVD

cache_expiry_days: 30
```

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

*Last updated: January 13, 2025.*
