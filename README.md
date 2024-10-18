# PhenoQC

**PhenoQC** is a lightweight, efficient, and user-friendly toolkit designed to perform comprehensive quality control (QC) on phenotypic datasets within the **Genomic Data Infrastructure (GDI)** framework. It ensures that phenotypic data adheres to standardized formats, maintains consistency, and is harmonized with recognized ontologies, thereby facilitating seamless integration with genomic data for advanced research.

## Features

- **Comprehensive Data Validation:** Checks format compliance, schema adherence, and data consistency.
- **Ontology Mapping:** Maps phenotypic terms to multiple standardized ontologies (HPO, DO, MPO) with synonym resolution and custom mapping support.
- **Missing Data Handling:** Detects and imputes missing data using simple strategies or flags for manual review.
- **Batch Processing:** Supports processing multiple files simultaneously with parallel execution.
- **User-Friendly Interfaces:** CLI for power users and an optional Streamlit-based GUI for interactive use.
- **Reporting and Visualization:** Generates detailed QC reports and visual summaries of data quality metrics.
- **Extensibility:** Modular design allows for easy addition of new validation rules or mapping functionalities.

## Installation

Ensure you have Python 3.6 or higher installed.

```bash
pip install phenoqc
```

Alternatively, clone the repository and install manually:

```bash
git clone https://github.com/jorgeMFS/PhenoQC.git
cd PhenoQC
pip install -e .
```

## Usage

### Command-Line Interface (CLI)

Process a single file:

```bash
phenoqc --input examples/samples/sample_data.json \
--output ./reports/ \
--schema examples/schemas/pheno_schema.json \
--config config.yaml \
--custom_mappings examples/mapping/custom_mappings.json \
--impute mice \
--unique_identifiers SampleID \
--ontologies HPO DO MPO

```

Batch process multiple files:

```bash
phenoqc --input examples/samples/sample_data.csv examples/samples/sample_data.json examples/samples/sample_data.tsv \
--output ./reports/ \
--schema examples/schemas/pheno_schema.json \
--config config.yaml \
--custom_mappings examples/mapping/custom_mappings.json \
--impute none \
--unique_identifiers SampleID \
--ontologies HPO DO MPO
```

**Parameters:**

- `--input`: One or more input data files or directories (supported formats: `csv`, `tsv`, `json`).
- `--output`: Directory to save reports and processed data. Defaults to `./reports/`.
- `--schema`: Path to the JSON schema file for data validation.
- `--config`: Path to the configuration YAML file (`config.yaml`) defining ontology mappings. Defaults to `config.yaml`.
- `--custom_mappings`: (Optional) Path to a custom mapping JSON file for ontology term resolutions.
- `--impute`: Strategy for imputing missing data. Choices:
  - `mean`: Impute missing numeric data with the column mean.
  - `median`: Impute missing numeric data with the column median.
  - `mode`: Impute missing categorical data with the column mode.
  - `knn`: Impute missing numeric data using k-Nearest Neighbors.
  - `mice`: Impute missing numeric data using Multiple Imputation by Chained Equations.
  - `svd`: Impute missing numeric data using Iterative Singular Value Decomposition.
  - `none`: Do not perform imputation; simply flag missing data.
- `--unique_identifiers`: List of column names that uniquely identify a record (e.g., `SampleID`).
- `--ontologies`: (Optional) List of ontologies to map to (e.g., `HPO DO MPO`).
- `--recursive`: (Optional) Enable recursive directory scanning when input paths include directories.


### Graphical User Interface (GUI)

Launch the GUI using Streamlit:

```bash
streamlit run src/gui.py
```

*Note: Ensure you have the GUI dependencies installed.*

**Steps:**

1. **Configuration:**
   - **Upload JSON Schema:** Upload your JSON schema file for data validation.
   - **Upload Configuration (`config.yaml`):** Upload the configuration file that defines the ontologies and their respective JSON files.
   - **Upload Custom Mapping (Optional):** (Optional) Upload a JSON file containing custom term mappings.
   - **Select Imputation Strategy:** Choose between 'mean' or 'median' for imputing missing data.

2. **Data Ingestion:**
   - **Select Data Source:** Choose between uploading individual phenotype data files or uploading a ZIP archive containing multiple files.
   - **Upload Files or ZIP:** Depending on the selected option, upload the necessary files.
   - **Enable Recursive Directory Scanning:** (Optional) Enable if you want the tool to scan directories recursively within the uploaded ZIP archive.

3. **Unique Identifiers & Ontologies:**
   - **Specify Unique Identifier Columns:** Enter column names that uniquely identify each record, separated by commas (e.g., `SampleID,PatientID`).
   - **Specify Ontologies to Map:** Enter ontology IDs separated by spaces (e.g., `HPO DO MPO`). Leave blank to use the default ontology specified in `config.yaml`.

4. **Run Quality Control:**
   - Click the "Run Quality Control" button to start processing.
   - View processing results and download generated reports.

## Configuration

PhenoQC uses a YAML configuration file (`config.yaml`) to specify ontology mappings and other settings. Ensure this file is properly set up in your project directory.

**Example `config.yaml`:**

```yaml
ontologies:
HPO:
name: Human Phenotype Ontology
file: ontologies/HPO.json
DO:
name: Disease Ontology
file: ontologies/DO.json
MPO:
name: Mammalian Phenotype Ontology
file: ontologies/MPO.json
default_ontology: HPO
```

Ensure that the ontology JSON files (`HPO.json`, `DO.json`, `MPO.json`) are correctly placed in the `ontologies/` directory and properly formatted.


## Documentation

Comprehensive documentation is available on the [GitHub Wiki](https://github.com/jorgeMFS/PhenoQC/wiki).

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
