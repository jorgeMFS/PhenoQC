# PhenoQC

**PhenoQC** is a lightweight, efficient, and user-friendly toolkit designed to perform comprehensive quality control (QC) on phenotypic datasets within the **Genomic Data Infrastructure (GDI)** framework. It ensures that phenotypic data adheres to standardized formats, maintains consistency, and is harmonized with recognized ontologies, thereby facilitating seamless integration with genomic data for advanced research.


## Features

- **Comprehensive Data Validation:** Checks format compliance, schema adherence, and data consistency.
- **Ontology Mapping:** Maps phenotypic terms to standardized ontologies like HPO with synonym resolution and custom mapping support.
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
phenoqc --input examples/sample_data.csv --output ./reports/ --schema schemas/pheno_schema.json --mapping examples/sample_mapping.json --impute mean
```

Batch process multiple files:

```bash
phenoqc --input examples/sample_data.csv examples/sample_data.json examples/sample_data.tsv --output ./reports/ --schema schemas/pheno_schema.json --mapping examples/sample_mapping.json --impute median
```

### Graphical User Interface (GUI)

Launch the GUI using Streamlit:

```bash
streamlit run src/gui.py
```

*Note: Ensure you have the GUI dependencies installed.*

## Configuration

PhenoQC can be configured using YAML or JSON configuration files. Refer to the `examples/` directory for sample configuration files.


## Documentation

Comprehensive documentation is available in the `docs/` directory and on the [GitHub Wiki](https://github.com/jorgeMFS/PhenoQC/wiki).

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


