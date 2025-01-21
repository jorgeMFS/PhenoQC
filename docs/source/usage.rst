Usage
==========================

Command Line Interface
----------------------------------------------------

PhenoQC provides a command-line interface for batch processing of phenotypic data files.

Basic Usage
--------------------

Process a single file:

.. code-block:: bash

    phenoqc \
      --input examples/samples/sample_data.json \
      --output ./reports/ \
      --schema examples/schemas/pheno_schema.json \
      --config config.yaml \
      --impute mice \
      --unique_identifiers SampleID \
      --phenotype_columns '{"PrimaryPhenotype": ["HPO"], "DiseaseCode": ["DO"]}' \
      --ontologies HPO DO

Batch Processing
~~~~~~~~~~~~~~

Process multiple files:

.. code-block:: bash

    phenoqc \
      --input examples/samples/sample_data.csv examples/samples/sample_data.json \
      --output ./reports/ \
      --schema examples/schemas/pheno_schema.json \
      --config config.yaml \
      --impute none \
      --unique_identifiers SampleID \
      --ontologies HPO DO MPO

Parameters
~~~~~~~~~

- ``--input``: One or more data files or directories (.csv, .tsv, .json, .zip)
- ``--output``: Directory for saving processed data and reports
- ``--schema``: Path to the JSON schema for data validation
- ``--config``: YAML config file defining ontologies and settings
- ``--custom_mappings``: Path to custom term-mapping JSON (optional)
- ``--impute``: Strategy for missing data (mean, median, mode, knn, mice, svd, none)
- ``--unique_identifiers``: Columns that uniquely identify each record
- ``--phenotype_columns``: JSON mapping of columns to ontologies
- ``--ontologies``: List of ontology IDs
- ``--recursive``: Enable recursive scanning of directories

Graphical User Interface
----------------------

Launch the GUI:

.. code-block:: bash

    python run_gui.py

The GUI provides an interactive interface for:

1. Uploading configuration and schema files
2. Uploading data files
3. Selecting unique identifiers and ontologies
4. Choosing missing data strategies
5. Running QC and viewing results

Configuration
------------

PhenoQC uses a YAML configuration file to define settings. Example ``config.yaml``:

.. code-block:: yaml

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

    default_ontologies:
      - HPO
      - DO

    fuzzy_threshold: 80
    cache_expiry_days: 30

    imputation_strategies:
      Age: mean
      Gender: mode
      Height: median

Output
------

PhenoQC generates:

1. Validated and processed data files
2. Quality control reports (PDF/Markdown)
3. Visual summaries of data quality
4. Detailed logs of the QC process

Troubleshooting
--------------

Common issues:

1. **Ontology Mapping Failures**: Check if config.yaml points to valid ontology URLs
2. **Missing Required Columns**: Ensure specified columns exist in the dataset
3. **Imputation Errors**: Verify column data types match imputation strategy
4. **Logs**: Check phenoqc_*.log for detailed error messages 