import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

# Parameters
num_records = 3000
missing_rate = 0.1
duplicate_rate = 0.05
output_path = '../study/synthetic_phenotypic_data.csv'

# Phenotypes and Ontology-related categories
phenotypes_primary = ['Phenotype A', 'Phenotype B', 'Phenotype C', 'Phenotype D', 'Phenotype Y']  # 'Y' might be known, 'X' unknown
phenotypes_secondary = ['Phenotype 1', 'Phenotype 2', 'Phenotype 3']
unmapped_phenotype = 'Phenotype X'  # Unmapped intentionally

# Introduce a complex categorical code system (like ICD or ontology codes)
disease_codes = ['DOID:1234', 'DOID:5678', 'DOID:9999']
unmapped_code = 'DOID:XXXX'  # Will appear in duplicates

# Valid genome sample and hospital IDs
valid_genome_ids = [f"GS_{i:05d}" for i in range(1, 2001)]
invalid_genome_ids = [f"GS_INVALID_{i}" for i in range(1, 101)]
valid_hospital_ids = [f"HID_{i:04d}" for i in range(1, 501)]
invalid_hospital_ids = [f"HID_BAD_{i}" for i in range(1, 51)]

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# Numeric Features (more extensive)
# Height (cm), Weight (kg), Cholesterol (mg/dL), Systolic BP (mmHg), Diastolic BP (mmHg),
# Glucose (mg/dL), Creatinine (mg/dL)
height = np.random.normal(170, 10, num_records)
weight = np.random.normal(70, 15, num_records)
cholesterol = np.random.normal(200, 40, num_records)
bp_systolic = np.random.normal(120, 15, num_records)
bp_diastolic = np.random.normal(80, 10, num_records)
glucose = np.random.normal(100, 20, num_records)
creatinine = np.random.normal(1.0, 0.3, num_records)  # mg/dL typical range ~0.6-1.2, can have outliers

# Introduce missingness and some extreme outliers in numeric features
for arr in [height, weight, cholesterol, bp_systolic, bp_diastolic, glucose, creatinine]:
    mask_missing = np.random.rand(num_records) < missing_rate
    arr[mask_missing] = np.nan

# Introduce outliers: super-high cholesterol in a few random records
outlier_indices = np.random.choice(num_records, size=5, replace=False)
cholesterol[outlier_indices] = np.random.choice([1000, 2000, 3000], size=5)

# Phenotype columns
primary_pheno = np.random.choice(phenotypes_primary, size=num_records)
secondary_pheno = np.random.choice(phenotypes_secondary, size=num_records)

# Missingness in phenotypes
mask_primary = np.random.rand(num_records) < missing_rate
primary_pheno[mask_primary] = None
mask_secondary = np.random.rand(num_records) < missing_rate
secondary_pheno[mask_secondary] = None

# Disease code column (ontology-like)
disease_col = np.random.choice(disease_codes, size=num_records)
mask_disease = np.random.rand(num_records) < missing_rate
disease_col[mask_disease] = None

# Introduce arrays of observed features (some mapped to ontology terms)
# We'll pick from phenotypes + some known synonyms + an unmapped term
feature_pool = ['Phenotype A', 'Phenotype B', 'Phenotype Synonym for B', 'Phenotype C', 'BadTerm', unmapped_phenotype]
def random_feature_array():
    length = np.random.randint(0, 5)  # Array length from 0 to 4
    if length == 0:
        return []  # empty array
    return np.random.choice(feature_pool, length).tolist()

feature_arrays = [random_feature_array() for _ in range(num_records)]
mask_feature_arrays = np.random.rand(num_records) < missing_rate
for i, m in enumerate(mask_feature_arrays):
    if m:
        feature_arrays[i] = None  # Entire array missing

# Date and Date-Time Columns
# VisitDate (date-only), SampleCollectionDate (date-time)
start_date = datetime.today() - timedelta(days=365*10)

visit_dates = []
collection_datetimes = []
for i in range(num_records):
    # VisitDate
    if np.random.rand() < missing_rate:
        visit_dates.append(None)
    else:
        if np.random.rand() < 0.95:
            # Valid date
            days_offset = np.random.randint(0, 365*10)
            valid_date = start_date + timedelta(days=days_offset)
            visit_dates.append(valid_date.strftime('%Y-%m-%d'))
        else:
            # Invalid date
            visit_dates.append(random.choice(["NOT_A_DATE", "2022-13-40", "0000-00-00", "31/02/2021"]))

    # SampleCollectionDate
    if np.random.rand() < missing_rate:
        collection_datetimes.append(None)
    else:
        if np.random.rand() < 0.95:
            # Valid date-time
            days_offset = np.random.randint(0, 365*10)
            seconds_offset = np.random.randint(0, 86400)  # random second in a day
            valid_dt = start_date + timedelta(days=days_offset, seconds=seconds_offset)
            collection_datetimes.append(valid_dt.isoformat())
        else:
            # Invalid date-time
            collection_datetimes.append("INVALID_DATETIME_99")

# GenomeSampleID and HospitalID
genome_ids = np.random.choice(valid_genome_ids + invalid_genome_ids, size=num_records)
hospital_ids = np.random.choice(valid_hospital_ids + invalid_hospital_ids, size=num_records)

# Missingness in these reference fields
mask_genome = np.random.rand(num_records) < missing_rate
mask_hospital = np.random.rand(num_records) < missing_rate
genome_ids[mask_genome] = None
hospital_ids[mask_hospital] = None

# Combine into DataFrame
df = pd.DataFrame({
    'SampleID': np.arange(1, num_records + 1),
    'Height_cm': height,
    'Weight_kg': weight,
    'Cholesterol_mgdl': cholesterol,
    'BP_systolic': bp_systolic,
    'BP_diastolic': bp_diastolic,
    'Glucose_mgdl': glucose,
    'Creatinine_mgdl': creatinine,
    'PrimaryPhenotype': primary_pheno,
    'SecondaryPhenotype': secondary_pheno,
    'DiseaseCode': disease_col,
    'ObservedFeatures': feature_arrays,  # array field
    'VisitDate': visit_dates,
    'SampleCollectionDateTime': collection_datetimes,
    'GenomeSampleID': genome_ids,
    'HospitalID': hospital_ids
})

# Introduce duplicates & conflicts
num_duplicates = int(num_records * duplicate_rate)
df_duplicates = df.sample(num_duplicates, random_state=42).copy()

if num_duplicates > 0:
    # Introduce conflicts in duplicated records:
    # 1st duplicated record: unmapped phenotype in primary
    df_duplicates.iloc[0, df_duplicates.columns.get_loc('PrimaryPhenotype')] = unmapped_phenotype
    
    # 2nd duplicated record: impossible numeric value (e.g. negative height)
    if num_duplicates > 1:
        df_duplicates.iloc[1, df_duplicates.columns.get_loc('Height_cm')] = -999
    
    # 3rd duplicated record: invalid disease code
    if num_duplicates > 2:
        df_duplicates.iloc[2, df_duplicates.columns.get_loc('DiseaseCode')] = unmapped_code
    
    # 4th duplicated record: entire feature array replaced with nonsense terms
    if num_duplicates > 3:
        df_duplicates.at[df_duplicates.index[3], 'ObservedFeatures'] = ["NonOntologyTerm1", "NonOntologyTerm2"]
    
    # Additional random changes:
    # 5th duplicated record (if exists): invalid GenomeSampleID
    if num_duplicates > 4:
        df_duplicates.iloc[4, df_duplicates.columns.get_loc('GenomeSampleID')] = "GS_MISSING_REF"
    
    # 6th duplicated record (if exists): invalid HospitalID
    if num_duplicates > 5:
        df_duplicates.iloc[5, df_duplicates.columns.get_loc('HospitalID')] = "HID_BAD_REF"

# Append duplicates and shuffle
df = pd.concat([df, df_duplicates], ignore_index=True)
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

# Ensure output directory and save
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df.to_csv(output_path, index=False)
print(f"Very complex synthetic dataset saved to {output_path}.")

# Summary of Improvements:
# - More numeric fields, each with missingness and some with extreme outliers.
# - Multiple phenotype columns referencing different ontology domains (primary, secondary, disease codes), including unmapped and misspelled terms.
# - An array field 'ObservedFeatures' containing multiple terms, some valid, some synonyms, some invalid, and missing in some rows.
# - Date and date-time fields with both valid and invalid entries.
# - Referential fields (GenomeSampleID, HospitalID) with valid and invalid values, plus missingness.
# - 5% duplicates with multiple types of conflicts and anomalies introduced.
#
# This dataset should provide a substantial challenge to the PhenoQC pipeline, exercising schema validation, ontology mapping, imputation, anomaly detection, referential integrity checks, handling of arrays, and advanced configurations.
