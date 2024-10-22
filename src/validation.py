import pandas as pd
import json
from typing import List, Dict, Any
import fastjsonschema  # Use fastjsonschema for faster validation

class DataValidator:
    def __init__(
        self,
        df: pd.DataFrame,
        schema: Dict[str, Any],
        unique_identifiers: List[str],
        reference_data: pd.DataFrame = None,
        reference_columns: List[str] = None
    ):
        """
        Initializes the DataValidator with the dataset, schema, unique identifiers, and reference data.

        :param df: pandas DataFrame containing the phenotypic data.
        :param schema: JSON schema dict to validate the data against.
        :param unique_identifiers: List of column names that uniquely identify a record.
        :param reference_data: pandas DataFrame containing the reference genomic records.
        :param reference_columns: List of columns in df to check against reference_data.
        """
        self.df = df
        self.schema = schema
        self.unique_identifiers = unique_identifiers
        self.reference_data = reference_data
        self.reference_columns = reference_columns
        self.duplicate_records = pd.DataFrame()
        self.conflicting_records = pd.DataFrame()
        self.integrity_issues = pd.DataFrame()
        self.referential_integrity_issues = pd.DataFrame()

        # Compile the schema once for faster validation
        self.validate_record = fastjsonschema.compile(self.schema)

    def validate_format(self) -> bool:
        """
        Validates the DataFrame against the provided JSON schema.

        :return: True if validation passes, False otherwise.
        """
        valid = True
        records = self.df.to_dict(orient='records')
        invalid_indices = []

        for idx, record in enumerate(records):
            try:
                self.validate_record(record)
            except fastjsonschema.JsonSchemaException as e:
                # Collect the index of invalid record
                invalid_indices.append(idx)
                valid = False

        if invalid_indices:
            # Collect invalid records
            self.integrity_issues = self.df.iloc[invalid_indices]

        return valid

    def identify_duplicates(self) -> pd.DataFrame:
        """
        Identifies duplicate records based on unique identifiers.

        :return: DataFrame containing duplicate records.
        """
        duplicates = self.df[self.df.duplicated(subset=self.unique_identifiers, keep=False)]
        self.duplicate_records = duplicates.sort_values(by=self.unique_identifiers)
        return self.duplicate_records

    def detect_conflicts(self) -> pd.DataFrame:
        """
        Detects conflicting information within the dataset for duplicate records.

        :return: DataFrame containing records with conflicting information.
        """
        if self.duplicate_records.empty:
            self.identify_duplicates()

        conflicting_records = pd.DataFrame()

        grouped = self.duplicate_records.groupby(self.unique_identifiers)
        for name, group in grouped:
            # Compare each column except unique identifiers
            duplicate_check = group.drop(columns=self.unique_identifiers).nunique(dropna=False) > 1
            if duplicate_check.any():
                conflicting_records = pd.concat([conflicting_records, group])

        self.conflicting_records = conflicting_records.drop_duplicates()
        return self.conflicting_records

    def verify_integrity(self) -> pd.DataFrame:
        """
        Ensures referential integrity and checks for corrupted or malformed data entries.

        :return: DataFrame containing records with integrity issues.
        """
        # Initialize a local DataFrame to collect integrity issues
        integrity_issues = pd.DataFrame()

        # 1. Correctly retrieve required fields from the root of the schema
        required_fields = self.schema.get('required', [])

        # Check for null values in required fields
        missing_required = self.df[self.df[required_fields].isnull().any(axis=1)]
        if not missing_required.empty:
            integrity_issues = pd.concat([integrity_issues, missing_required])

        # 2. Check data types and additional constraints
        type_mapping = {
            'integer': int,
            'number': (int, float),
            'string': str,
            'array': list,
            'object': dict,
            'boolean': bool,
            'null': type(None)
            # Add more mappings as needed
        }

        for column, props in self.schema.get('properties', {}).items():
            expected_type = props.get('type')
            if expected_type:
                if isinstance(expected_type, list):
                    valid_types = []
                    for t in expected_type:
                        if t == 'null':
                            valid_types.append(type_mapping['null'])
                        elif t in type_mapping:
                            if isinstance(type_mapping[t], tuple):
                                valid_types.extend(type_mapping[t])
                            else:
                                valid_types.append(type_mapping[t])
                    valid_types = tuple(valid_types)
                    # Identify rows with invalid types
                    invalid_type = self.df[~self.df[column].apply(lambda x: isinstance(x, valid_types))]
                else:
                    python_type = type_mapping.get(expected_type)
                    if python_type:
                        invalid_type = self.df[~self.df[column].apply(lambda x: isinstance(x, python_type) or pd.isnull(x))]
                    else:
                        continue  # Unsupported type, skip
                if not invalid_type.empty:
                    integrity_issues = pd.concat([integrity_issues, invalid_type])

            # 3. Check additional constraints like 'minimum'
            if 'minimum' in props:
                min_value = props['minimum']

                # Only perform 'minimum' check if the column is of a numeric type
                if expected_type in ['number', 'integer']:
                    # Convert column to numeric, coercing errors to NaN
                    numeric_series = pd.to_numeric(self.df[column], errors='coerce')

                    # Identify rows where conversion failed (i.e., non-numeric entries)
                    non_numeric = self.df[column].where(numeric_series.isna())
                    if not non_numeric.dropna().empty:
                        integrity_issues = pd.concat([integrity_issues, self.df[numeric_series.isna()]])

                    # Now, safely perform the 'minimum' comparison using the numeric_series
                    mask = numeric_series < min_value
                    below_min = self.df[mask]
                    if not below_min.empty:
                        integrity_issues = pd.concat([integrity_issues, below_min])

        # Append new integrity issues
        self.integrity_issues = pd.concat([self.integrity_issues, integrity_issues]).drop_duplicates()

        # 4. Referential Integrity Check
        if self.reference_data is not None and self.reference_columns is not None:
            self.check_referential_integrity()

        return self.integrity_issues

    def check_referential_integrity(self):
        """
        Checks if the data references exist in the reference dataset.

        Populates self.referential_integrity_issues with records that have missing references.
        """
        # For each reference column, check if the values exist in the reference data
        for column in self.reference_columns:
            if column in self.df.columns and column in self.reference_data.columns:
                missing_refs = self.df[~self.df[column].isin(self.reference_data[column])]
                if not missing_refs.empty:
                    self.referential_integrity_issues = pd.concat([self.referential_integrity_issues, missing_refs])
            else:
                print(f"Column '{column}' not found in both data and reference data.")

        # Remove duplicates
        self.referential_integrity_issues = self.referential_integrity_issues.drop_duplicates()

    def run_all_validations(self) -> Dict[str, Any]:
        """
        Runs all validation checks.

        :return: Dictionary containing results of all validation checks.
        """
        # Run format validation
        format_valid = self.validate_format()

        # Identify duplicates
        duplicates = self.identify_duplicates()

        # Detect conflicts
        conflicts = self.detect_conflicts()

        # Verify integrity (includes referential integrity if applicable)
        verify_integrity_issues = self.verify_integrity()

        # Combine all integrity issues (from format validation and verify_integrity)
        all_integrity_issues = self.integrity_issues.copy()

        # Include referential integrity issues
        if not self.referential_integrity_issues.empty:
            all_integrity_issues = pd.concat([all_integrity_issues, self.referential_integrity_issues]).drop_duplicates()

        results = {
            "Format Validation": format_valid,
            "Duplicate Records": duplicates,
            "Conflicting Records": conflicts,
            "Integrity Issues": all_integrity_issues,
            "Referential Integrity Issues": self.referential_integrity_issues
        }
        return results
