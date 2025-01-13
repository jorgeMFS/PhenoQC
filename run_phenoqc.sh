#!/usr/bin/env bash
set -e

# This script sets up a Python virtual environment, installs dependencies, and runs PhenoQC on your dataset.
# Adjust paths as necessary.

DATASET_PATH="./study/synthetic_phenotypic_data.csv"
SCHEMA_PATH="./scripts/config/schema.json"
CONFIG_PATH="./scripts/config/config.yaml"
OUTPUT_DIR="./reports/"
CUSTOM_MAPPINGS=""

# Optional: If you have a custom mapping file, specify its path here
# CUSTOM_MAPPINGS="path/to/custom_mappings.json"

# # 1. Set up virtual environment (if not already done)
# if [ ! -d "venv" ]; then
#     python3 -m venv venv
# fi
# source venv/bin/activate

# # 2. Install the requirements if needed
# # Make sure that your code and requirements are properly listed in a requirements.txt file
# pip install --upgrade pip
# pip install -r requirements.txt

#3. Run the PhenoQC tool
#Ensure phenoqc CLI entry point is installed. If not, you can run `python src/cli.py ...` instead.
phenoqc \
    --input "$DATASET_PATH" \
    --output "$OUTPUT_DIR" \
    --schema "$SCHEMA_PATH" \
    --config "$CONFIG_PATH" \
    --impute mean \
    --unique_identifiers SampleID \
    --ontologies HPO DO MPO \
    --phenotype_column PrimaryPhenotype \
    ${CUSTOM_MAPPINGS:+--custom_mappings "$CUSTOM_MAPPINGS"}

echo "Processing completed. Check $OUTPUT_DIR for reports."
