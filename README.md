# PhenoQC

**PhenoQC** is a lightweight, efficient, and user-friendly toolkit designed to perform comprehensive quality control (QC) on phenotypic datasets. It ensures that phenotypic data adheres to standardized formats, maintains consistency, and is harmonized with recognized ontologies, thereby facilitating seamless integration with genomic data for advanced research.

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

### Using `pip`

```bash
pip install phenoqc
```

### Manual Installation

Alternatively, clone the repository and install manually:

```bash
git clone https://github.com/jorgeMFS/PhenoQC.git
cd PhenoQC
pip install -e .
```

### Dependencies

PhenoQC relies on several Python packages. Ensure they are installed either via `pip` or included in the `requirements.txt`:

- pandas
- jsonschema
- requests
- plotly
- reportlab
- streamlit
- pyyaml
- watchdog
- kaleido>=0.1.0
- tqdm
- Pillow
- scikit-learn
- fancyimpute
- fastjsonschema
- pronto
- rapidfuzz

## Usage

### Command-Line Interface (CLI)

PhenoQC provides a powerful CLI for processing phenotypic data files efficiently.

#### Process a Single File

```bash
phenoqc --input examples/samples/sample_data.json \
--output ./reports/ \
--schema examples/schemas/pheno_schema.json \
--config config.yaml \
--custom_mappings examples/mapping/custom_mappings.json \
--impute mice \
--unique_identifiers SampleID \
--phenotype_columns '{"PrimaryPhenotype": ["HPO"], "DiseaseCode": ["DO"], "TertiaryPhenotype": ["MPO"]}' \
--ontologies HPO DO MPO
```

#### Batch Process Multiple Files

```bash
phenoqc --input examples/samples/sample_data.csv examples/samples/sample_data.json examples/samples/sample_data.tsv \
--output ./reports/ \
--schema examples/schemas/pheno_schema.json \
--config config.yaml \
--custom_mappings examples/mapping/custom_mappings.json \
--impute none \
--unique_identifiers SampleID \
--ontologies HPO DO MPO \
--phenotype_columns '{"PrimaryPhenotype": ["HPO"], "DiseaseCode": ["DO"], "TertiaryPhenotype": ["MPO"]}'
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
- `--phenotype_columns`: JSON mapping of columns to their target ontologies. Format: `{"ColumnName": ["OntologyID"], ...}`. Examples:
  - Single column: `{"PrimaryPhenotype": ["HPO"]}`
  - Multiple columns: `{"PrimaryPhenotype": ["HPO"], "DiseaseCode": ["DO"], "TertiaryPhenotype": ["MPO"]}`
  - Multiple ontologies per column: `{"PrimaryPhenotype": ["HPO", "DO"]}`

### Graphical User Interface (GUI)

PhenoQC also offers a Streamlit-based GUI for users who prefer an interactive experience.

#### Launch the GUI

```bash
streamlit run src/gui.py
```

**Steps:**

1. **Configuration:**
   - **Upload JSON Schema:** Upload your JSON schema file for data validation.
   - **Upload Configuration (`config.yaml`):** Upload the configuration file that defines the ontologies and their respective JSON files.
   - **Upload Custom Mapping (Optional):** (Optional) Upload a JSON file containing custom term mappings.
   - **Select Imputation Strategy:** Choose between available imputation strategies for handling missing data.

2. **Data Ingestion:**
   - **Select Data Source:** Choose between uploading individual phenotype data files or uploading a ZIP archive containing multiple files.
   - **Upload Files or ZIP:** Depending on the selected option, upload the necessary files.
   - **Enable Recursive Directory Scanning:** (Optional) Enable if you want the tool to scan directories recursively within the uploaded ZIP archive.

3. **Unique Identifiers & Ontologies:**
   - **Specify Unique Identifier Columns:** Select column names that uniquely identify each record (e.g., `SampleID,PatientID`).
   - **Specify Phenotype Columns:** Map phenotype columns to their target ontologies using either:
     - JSON editor: Enter mappings in JSON format (e.g., `{"PrimaryPhenotype": ["HPO"], "DiseaseCode": ["DO"]}`)
     - Visual editor: Use the interactive interface to map columns to ontologies
   - **Specify Ontologies to Map:** Select ontology IDs to map to (e.g., `HPO DO MPO`). Leave blank to use the default ontologies specified in `config.yaml`.

4. **Run Quality Control:**
   - Click the "Run Quality Control" button to start processing.
   - View processing results and download generated reports.

## Configuration

PhenoQC uses a YAML configuration file (`config.yaml`) to specify ontology mappings and other settings. Ensure this file is properly set up in your project directory.

### Example `config.yaml`:

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

**Configuration Parameters:**

- `ontologies`: Defines the ontologies to be used for mapping, including their sources and formats.
- `default_ontologies`: Specifies which ontologies to map to by default.
- `fuzzy_threshold`: Sets the threshold for fuzzy matching when mapping terms (default is 80).
- `imputation_strategies`: Defines default and column-specific strategies for imputing missing data.
- `advanced_imputation_methods`: Lists advanced imputation methods available.
- `cache_expiry_days`: Determines how long ontology files are cached before re-downloading (default is 30 days).

### Ontology Configuration

Ensure that the ontology JSON files (`HPO.json`, `DO.json`, `MPO.json`) are correctly specified in the `config.yaml` and accessible either locally or via the provided URLs. The tool will handle downloading and caching these files based on the configuration.

## Handling Encoding Issues

PhenoQC processes various data file formats (`csv`, `tsv`, `json`). It's essential to ensure that these files are encoded in UTF-8 to prevent decoding errors. If you encounter errors such as:

```
Error reading file: 'utf-8' codec can't decode byte 0xe3 in position 16: invalid continuation byte
```

**Solutions:**

1. **Verify File Encoding:**
   - Ensure your data files are saved with UTF-8 encoding. Most modern text editors allow you to check and set the file encoding.

2. **Re-encode Files:**
   - Use tools like `iconv` to convert files to UTF-8:

     ```bash
     iconv -f ISO-8859-1 -t UTF-8 input_file.csv -o output_file.csv
     ```

3. **Handle in Code:**
   - Modify the data reading functions to specify the correct encoding or handle errors gracefully. For example:

     ```python
     pd.read_csv('file.csv', encoding='utf-8', errors='replace')
     ```

4. **Check for Binary Data:**
   - Ensure that your data files do not contain binary data or corrupted characters that could disrupt the decoding process.

## Troubleshooting

### Common Issues

1. **UTF-8 Decoding Errors:**
   - As mentioned above, ensure all input files are properly encoded in UTF-8.

2. **Ontology Mapping Failures:**
   - Verify that the ontology URLs in `config.yaml` are accessible.
   - Ensure that the ontology files are correctly formatted and not corrupted.

3. **Missing Required Columns:**
   - Ensure that all columns specified as unique identifiers or phenotypic terms exist in your data files.

4. **Imputation Strategy Not Applicable:**
   - Certain imputation strategies like `mean` or `median` are only applicable to numeric columns. Ensure that you select appropriate strategies based on your data types.

### Logs and Debugging

PhenoQC maintains a log file (`phenoqc.log`) that records all activities and errors. Review this file to identify and troubleshoot issues.

**Location:**

- By default, the log file is saved in the project's root directory. Ensure you have write permissions to create and modify this file.

### Support

If you encounter issues not covered in this guide, please consider the following steps:

1. **Check the Documentation:**
   - Comprehensive documentation is available on the [GitHub Wiki](https://github.com/jorgeMFS/PhenoQC/wiki).

2. **Review Log Files:**
   - Examine the `phenoqc.log` file for detailed error messages and stack traces.

3. **Seek Help:**
   - Open an issue on the [GitHub Issues Page](https://github.com/jorgeMFS/PhenoQC/issues) with a detailed description of your problem, steps to reproduce, and any relevant log excerpts.

## Contributing

Contributions are welcome! PhenoQC is an open-source project, and your enhancements can help improve its functionality and usability.

**Steps to Contribute:**

1. **Fork the Repository:**
   - Click the "Fork" button on the [GitHub repository](https://github.com/jorgeMFS/PhenoQC).

2. **Create a Feature Branch:**

   ```bash
     git checkout -b feature/YourFeatureName
     ```

3. **Commit Your Changes:**

   ```bash
     git commit -m "Add some feature"
     ```

4. **Push to the Branch:**

   ```bash
     git push origin feature/YourFeatureName
     ```

5. **Open a Pull Request:**
   - Navigate to your forked repository and click "Compare & pull request."

6. **Describe Your Changes:**
   - Provide a clear description of what you've done and why.

7. **Await Review:**
   - The maintainers will review your pull request and provide feedback.

**Guidelines:**

- Ensure that your code follows the project's coding standards and style.
- Write clear and concise commit messages.
- Include tests for new features or bug fixes.
- Update documentation as necessary.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- **Human Phenotype Ontology (HPO):** [http://purl.obolibrary.org/obo/hp.obo](http://purl.obolibrary.org/obo/hp.obo)
- **Disease Ontology (DO):** [http://purl.obolibrary.org/obo/doid.obo](http://purl.obolibrary.org/obo/doid.obo)
- **Mammalian Phenotype Ontology (MPO):** [http://purl.obolibrary.org/obo/mp.obo](http://purl.obolibrary.org/obo/mp.obo)

## Contact

For any inquiries or feedback, please contact **Jorge Miguel Ferreira da Silva** at [jorge(dot)miguel(dot)ferreira(dot)silva@ua.pt](mailto:jorge(dot)miguel(dot)ferreira(dot)silva@ua.pt).

---

*This README was last updated on October 25, 2024.*

---

## Additional Notes

- **Extensibility:** To add new ontologies, update the `config.yaml` with the ontology's details and ensure the ontology file is accessible.
- **Caching:** Ontology files are cached locally to reduce redundant downloads. The cache expiry can be adjusted via the `cache_expiry_days` parameter in `config.yaml`.
- **Custom Mappings:** Utilize the `--custom_mappings` parameter or upload a custom mapping file via the GUI to handle specific term mappings not covered by standard ontologies.

