import json
import pandas as pd
from typing import List, Dict, Any, Optional, Union
import fastjsonschema
import re
from datetime import datetime
from .logging_module import log_activity

class DataValidator:
    """
    A comprehensive DataValidator that performs both row-level JSON schema validation
    and cell-level property checks, along with detection of duplicates, conflicts,
    referential integrity, and anomalies.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        schema: Dict[str, Any],
        unique_identifiers: List[str],
        reference_data: pd.DataFrame = None,
        reference_columns: List[str] = None
    ):
        """
        Args:
            df (pd.DataFrame): The phenotypic data to validate.
            schema (dict): A JSON schema dict describing expected fields, types, constraints, etc.
            unique_identifiers (list): Column names that uniquely identify a record.
            reference_data (pd.DataFrame, optional): A reference dataset for cross-checking references (if any).
            reference_columns (list, optional): Which columns in `df` must match `reference_data`.
        """
        self.df = df
        self.schema = schema
        self.unique_identifiers = unique_identifiers
        self.reference_data = reference_data
        self.reference_columns = reference_columns

        # DataFrames for issues found
        self.duplicate_records = pd.DataFrame()
        self.conflicting_records = pd.DataFrame()
        self.integrity_issues = pd.DataFrame()
        self.referential_integrity_issues = pd.DataFrame()
        self.anomalies = pd.DataFrame()

        # Compile the JSON schema for row-level validation
        self.validate_record = fastjsonschema.compile(self.schema)

        # A mask for cell-level validation: same shape as df, True where invalid
        self.invalid_mask = pd.DataFrame(False, index=df.index, columns=df.columns)

    # -------------------------------------------------------------------------
    # 1. Row-Level Validation with JSON Schema
    # -------------------------------------------------------------------------

    def validate_format_rowwise(self) -> bool:
        """
        Checks each row as a whole against the JSON schema.
        If a row fails, we note it in `self.integrity_issues`
        and mark a 'SchemaViolationFlag' in self.df.

        Returns:
            bool: True if all rows pass, False if any row fails.
        """
        valid = True
        # Instead of orient='records', use 'index' so keys = actual row indices
        records = self.df.to_dict(orient='index')

        # If not present, add a column to mark row-level violations
        if 'SchemaViolationFlag' not in self.df.columns:
            self.df['SchemaViolationFlag'] = False

        invalid_indices = []

        for row_idx, record in records.items():
            try:
                self.validate_record(record)  # raises if invalid
            except fastjsonschema.JsonSchemaException as e:
                invalid_indices.append(row_idx)
                preview = str(record)[:300]
                msg = (
                    f"[SchemaValidation] Row #{row_idx} failed: {e.message}. "
                    f"Record snippet: {preview}"
                )
                print("[DEBUG] Full JSON Schema exception for row:", row_idx)
                print("[DEBUG] Exception detail:", e.__dict__)  # might show 'path', 'rule_definition', etc.
                log_activity(msg, level='warning')
                valid = False
                if getattr(e, 'path', None):
                    for key_in_path in e.path:
                        if key_in_path in self.df.columns:
                            self.invalid_mask.at[row_idx, key_in_path] = True
                else:
                    self.invalid_mask.loc[row_idx, :] = True
                

        if invalid_indices:
            # Mark those rows as having schema violations
            self.df.loc[invalid_indices, 'SchemaViolationFlag'] = True
            # Store them for reporting
            violators = self.df.loc[invalid_indices]
            self.integrity_issues = pd.concat([self.integrity_issues, violators]).drop_duplicates()

        return valid

    # -------------------------------------------------------------------------
    # 2. Cell-Level Validation
    # -------------------------------------------------------------------------

    def validate_cells(self):
        """
        Checks each cell in self.df against the schema's "properties" constraints
        such as: type, minimum, format, etc.

        We store True in `self.invalid_mask[row, col]` if that cell fails.
        """
        props = self.schema.get('properties', {})

        for col, col_rules in props.items():
            if col not in self.df.columns:
                # No such column in df
                continue

            expected_type = col_rules.get('type')
            min_val = col_rules.get('minimum')
            fmt = col_rules.get('format')

            for idx, value in self.df[col].items():
                # 1) Type check
                if not self._passes_type_check(value, expected_type):
                    self.invalid_mask.at[idx, col] = True
                    continue

                # 2) Minimum check
                if min_val is not None and isinstance(value, (int, float)):
                    if value < min_val:
                        self.invalid_mask.at[idx, col] = True
                        continue

                # 3) Format check
                if fmt is not None:
                    if not self._check_format(value, fmt):
                        self.invalid_mask.at[idx, col] = True
                        continue

        return self.invalid_mask

    def _passes_type_check(self, value, expected_type) -> bool:
        """
        Basic helper to see if 'value' matches the JSON schema's expected_type.
        """
        if not expected_type:
            return True  # no constraint on type

        if isinstance(expected_type, list):
            # e.g. ["string", "null"]
            return any(self._single_type_check(value, t) for t in expected_type)
        else:
            return self._single_type_check(value, expected_type)

    def _single_type_check(self, value, t) -> bool:
        """
        Check if a value matches a single JSON schema type.
        """
        if t == 'null':
            return value is None
        elif t == 'string':
            return isinstance(value, str) or value is None
        elif t == 'number':
            return isinstance(value, (int, float)) or value is None
        elif t == 'integer':
            return isinstance(value, int) or value is None
        elif t == 'boolean':
            return isinstance(value, bool) or value is None
        elif t == 'array':
            return (
                isinstance(value, (list, tuple, pd.Series))
                or hasattr(value, '__iter__')  # or any other check you want
                or value is None
            )
        elif t == 'object':
            return (
                isinstance(value, (dict, pd.DataFrame))
                or (hasattr(value, '__dict__') and not isinstance(value, type))
                or value is None
            )
        elif t == 'date':
            if value is None:
                return True
            try:
                if isinstance(value, str):
                    datetime.strptime(value, '%Y-%m-%d')
                elif isinstance(value, datetime):
                    return True
                return False
            except ValueError:
                return False
        elif t == 'date-time':
            if value is None:
                return True
            try:
                if isinstance(value, str):
                    pd.to_datetime(value)
                elif isinstance(value, datetime):
                    return True
                return False
            except (ValueError, TypeError):
                return False

        # Unknown type - log warning and pass
        log_activity(f"Unknown type '{t}' in schema. Allowing value.", level='warning')
        return True

    def _check_format(self, value, fmt) -> bool:
        """
        Check special format constraints from JSON Schema.
        """
        import re
        if value is None:
            return True

        if fmt == 'date':
            pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
            return bool(pattern.match(str(value)))
        elif fmt == 'date-time':
            try:
                pd.to_datetime(value, errors='raise')
                return True
            except (ValueError, TypeError):
                return False
        elif fmt == 'time':
            pattern = re.compile(r'^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)')
            return bool(pattern.match(str(value)))
        elif fmt == 'email':
            pattern = re.compile(r'^[^@]+@[^@]+\.[^@]+$')
            return bool(pattern.match(str(value)))
        elif fmt == 'uri':
            from urllib.parse import urlparse
            result = urlparse(str(value))
            return all([result.scheme, result.netloc])
        elif fmt == 'uuid':
            pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
            return bool(pattern.match(str(value)))
        elif fmt == 'identifier':
            # E.g. "HP:0000822"
            pattern = re.compile(r'^[A-Z]+:\d+$')
            return bool(pattern.match(str(value)))
        elif fmt == 'percentage':
            try:
                float_val = float(str(value).rstrip('%'))
                return 0 <= float_val <= 100
            except (ValueError, TypeError):
                return False
        elif fmt == 'phone':
            pattern = re.compile(r'^\+?[\d\s-]{10,}$')
            return bool(pattern.match(str(value)))

        log_activity(f"Unknown format '{fmt}' requested. Allowing value.", level='warning')
        return True

    # -------------------------------------------------------------------------
    # 3. Duplicate and Conflict Detection
    # -------------------------------------------------------------------------

    def identify_duplicates(self) -> pd.DataFrame:
        """
        Identifies rows that share the same unique_identifiers.
        """
        dups = self.df[self.df.duplicated(subset=self.unique_identifiers, keep=False)]
        self.duplicate_records = dups.sort_values(by=self.unique_identifiers)
        return self.duplicate_records

    def detect_conflicts(self) -> pd.DataFrame:
        """
        Among the identified duplicates, detects rows that have conflicting info
        in columns other than unique_identifiers.
        """
        if self.duplicate_records.empty:
            self.identify_duplicates()

        conflict_rows = []
        grouped = self.duplicate_records.groupby(self.unique_identifiers)
        for _, group in grouped:
            non_id_cols = [c for c in group.columns if c not in self.unique_identifiers]
            # If any non-ID column has >1 unique value => conflict
            if (group[non_id_cols].nunique(dropna=False) > 1).any():
                conflict_rows.append(group)

        if conflict_rows:
            self.conflicting_records = pd.concat(conflict_rows).drop_duplicates()
        return self.conflicting_records

    # -------------------------------------------------------------------------
    # 4. Referential Integrity
    # -------------------------------------------------------------------------

    def verify_integrity(self) -> pd.DataFrame:
        """
        Checks for required fields, typed constraints, referential integrity, etc.
        """
        integrity_issues_local = pd.DataFrame()

        # A) Check for missing required fields
        required_fields = self.schema.get('required', [])
        if required_fields:
            missing_required = self.df[self.df[required_fields].isnull().any(axis=1)]
            if not missing_required.empty:
                integrity_issues_local = pd.concat([integrity_issues_local, missing_required])

        # B) Additional checks (or rely on row/cell logic)
        # ...

        # C) Check referential integrity if reference_data is provided
        if self.reference_data is not None and self.reference_columns is not None:
            self.check_referential_integrity()

        if not integrity_issues_local.empty:
            self.integrity_issues = pd.concat(
                [self.integrity_issues, integrity_issues_local]
            ).drop_duplicates()

        return self.integrity_issues

    def check_referential_integrity(self):
        """
        Ensures that values in self.reference_columns exist in self.reference_data.
        """
        if not self.reference_data or not self.reference_columns:
            log_activity("No reference data/columns, skipping referential checks.", level='info')
            return

        for col in self.reference_columns:
            if col not in self.df.columns or col not in self.reference_data.columns:
                log_activity(f"Column {col} not in both df and reference_data. Skipping...", level='warning')
                continue

            missing_refs = self.df[~self.df[col].isin(self.reference_data[col])]
            if not missing_refs.empty:
                self.referential_integrity_issues = pd.concat([
                    self.referential_integrity_issues,
                    missing_refs
                ]).drop_duplicates()

    # -------------------------------------------------------------------------
    # 5. Anomaly Detection (Outliers)
    # -------------------------------------------------------------------------

    def detect_anomalies(self):
        """
        Simple numeric outlier detection using Z-score>3 as a threshold.
        """
        numeric_cols = self.df.select_dtypes(include=['number']).columns

        for col in numeric_cols:
            mean_ = self.df[col].mean()
            std_ = self.df[col].std()
            if pd.isnull(std_) or std_ == 0:
                # no variability => skip
                continue

            z_scores = (self.df[col] - mean_) / std_
            outliers = self.df[abs(z_scores) > 3]
            if not outliers.empty:
                self.anomalies = pd.concat([self.anomalies, outliers])

        self.anomalies.drop_duplicates(inplace=True)

    # -------------------------------------------------------------------------
    # 6. Orchestrator
    # -------------------------------------------------------------------------

    def run_all_validations(self) -> Dict[str, Any]:
        """
        Runs row-level validation, cell-level checks, duplicates, conflicts,
        referential checks, and anomaly detection.
        """
        format_valid = self.validate_format_rowwise()
        self.validate_cells()
        dups = self.identify_duplicates()
        conflicts = self.detect_conflicts()
        self.verify_integrity()
        self.detect_anomalies()

        combined_issues = pd.concat([
            self.integrity_issues, self.referential_integrity_issues
        ]).drop_duplicates()

        return {
            "Format Validation": format_valid,
            "Duplicate Records": dups,
            "Conflicting Records": conflicts,
            "Integrity Issues": combined_issues,
            "Referential Integrity Issues": self.referential_integrity_issues,
            "Anomalies Detected": self.anomalies,
            "Invalid Mask": self.invalid_mask,
        }
