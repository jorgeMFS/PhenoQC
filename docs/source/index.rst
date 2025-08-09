Welcome to PhenoQC's documentation!
==================================

PhenoQC is a lightweight, efficient, and user-friendly toolkit designed to perform comprehensive quality control (QC) on phenotypic datasets. It ensures that data adheres to standardized formats, maintains consistency, and is harmonized with recognized ontologies—facilitating seamless integration with genomic data for advanced research.

Key Features
----------------------------------------------------

* **Comprehensive Data Validation:** Checks format compliance, schema adherence, and data consistency against JSON schemas.
* **Ontology Mapping:** Maps phenotypic terms to standardized ontologies (HPO, DO, MPO) with synonym resolution and optional custom mappings.
* **Missing Data Handling:** Detects and optionally imputes missing data using various strategies (mean, median, mode, KNN, MICE, SVD).
* **Batch Processing:** Processes multiple files simultaneously in parallel.
* **User-Friendly Interfaces:** Provides both CLI and GUI interfaces.
* **Reporting and Visualization:** Generates detailed QC reports and visualizations with improved formatting.
* **Class Distribution (Optional):** Provide a label column to get a class-imbalance summary and warning.
* **Imputation Parameters & Quick Tuning:** Configure imputation parameters and optionally run a mask-and-score tuner.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   usage
   api
   contributing
   changelog

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search` 