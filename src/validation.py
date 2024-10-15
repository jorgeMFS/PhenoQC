import pandas as pd
import json
from typing import List, Dict, Any
from jsonschema import validate, ValidationError

class DataValidator:
    def __init__(self, df: pd.DataFrame, schema: Dict[str, Any], unique_identifiers: List[str]):
        """
        Initializes the DataValidator with the dataset, schema, and unique identifiers.

        :param df: pandas DataFrame containing the phenotypic data.
        :param schema: JSON schema dict to validate the data against.
        :param unique_identifiers: List of column names that uniquely identify a record.
        """
        self.df = df
        self.schema = schema
        self.unique_identifiers = unique_identifiers
        self.duplicate_records = pd.DataFrame()
        self.conflicting_records = pd.DataFrame()
        self.integrity_issues = pd.DataFrame()

    def validate_format(self) -> bool:
        """
        Validates the DataFrame against the provided JSON schema.

        :return: True if validation passes, False otherwise.
        """
        valid = True
        for index, row in self.df.iterrows():
            try:
                record = row.to_dict()
                validate(instance=record, schema=self.schema)
            except ValidationError as ve:
                print(f"Format Validation Error at row {index}: {ve.message}")
                # Collect the invalid record
                self.integrity_issues = pd.concat([self.integrity_issues, self.df.loc[[index]]])
                valid = False
            except Exception as e:
                print(f"Unexpected error during format validation at row {index}: {e}")
                self.integrity_issues = pd.concat([self.integrity_issues, self.df.loc[[index]]])
                valid = False

        self.integrity_issues = self.integrity_issues.drop_duplicates()
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

        # **Modify the following line to append new integrity issues instead of overwriting**
        self.integrity_issues = pd.concat([self.integrity_issues, integrity_issues]).drop_duplicates()
        return self.integrity_issues

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

        # Verify integrity
        verify_integrity_issues = self.verify_integrity()

        # Combine all integrity issues (from format validation and verify_integrity)
        all_integrity_issues = self.integrity_issues.copy()

        results = {
            "Format Validation": format_valid,
            "Duplicate Records": duplicates,
            "Conflicting Records": conflicts,
            "Integrity Issues": all_integrity_issues
        }
        return results