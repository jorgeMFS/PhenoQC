import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

# ---------------------
# Parameters
# ---------------------
num_records = 3000
missing_rate = 0.1
duplicate_rate = 0.05
output_path = 'output/synthetic_phenotypic_data.csv'

# --------------------------------------------------
# HPO terms + synonyms for PRIMARY phenotype
# (some valid, some invalid, partial synonyms)
# --------------------------------------------------
valid_hpo_terms = [
    "HP:0001250",  # Seizures
    "HP:0001629",  # Hypertension
    "HP:0004322",  # Hypoglycemia
    "HP:0003003",  # Short stature
    "HP:0001249",  # Intellectual disability
    "HP:0001631",  # Tachycardia
    "HP:0012722",  # Elevated blood pressure
]
invalid_hpo_terms = [
    "HP:9999999",  # invalid
    "HP:ZZZZZZZ",  # invalid format
    "RubbishTerm", # random nonsense
]

# Real HPO synonyms for the *primary* phenotype column
hpo_synonyms_primary = {
    "HP:0001250": ["Seizures", "Epileptic episodes"],
    "HP:0001629": ["Hypertension", "High blood pressure"],
    "HP:0004322": ["Hypoglycemia", "Low blood sugar"],
    "HP:0003003": ["Short stature"],
    "HP:0001249": ["Intellectual disability"],
    "HP:0001631": ["Tachycardia", "Fast heart rate"],
    "HP:0012722": ["Elevated blood pressure", "Raised BP"],
}

def random_hpo_label_primary():
    # ~70% valid, ~30% invalid
    if random.random() < 0.7:
        term = random.choice(valid_hpo_terms)
        # 50% chance: use the ID itself or one of the synonyms
        if random.random() < 0.5 and term in hpo_synonyms_primary:
            return random.choice(hpo_synonyms_primary[term])
        else:
            return term
    else:
        return random.choice(invalid_hpo_terms)

# --------------------------------------------------
# Additional HPO terms + synonyms for SECONDARY phenotype
# --------------------------------------------------
valid_hpo_terms_secondary = [
    "HP:0002018",  # Constipation
    "HP:0001508",  # Obesity
    "HP:0002013",  # Diarrhea
    "HP:0001257",  # Ataxia
    "HP:0002354",  # Fatigue
    "HP:0001382",  # Joint pain (Arthralgia)
    "HP:0000750",  # Anxiety
]
invalid_secondary = [
    "Secondary_X", 
    "Phenotype_Junk",
    "SomeInvalidTerm",
]

hpo_synonyms_secondary = {
    "HP:0002018": ["Constipation"],
    "HP:0001508": ["Obesity", "Overweight"],
    "HP:0002013": ["Diarrhea"],
    "HP:0001257": ["Ataxia"],
    "HP:0002354": ["Fatigue", "Chronic tiredness"],
    "HP:0001382": ["Joint pain", "Arthralgia"],
    "HP:0000750": ["Anxiety", "Nervousness"],
}

def random_hpo_label_secondary():
    # ~80% valid, ~20% invalid
    if random.random() < 0.8:
        term = random.choice(valid_hpo_terms_secondary)
        # 50% chance: use the ID or a synonym
        if random.random() < 0.5 and term in hpo_synonyms_secondary:
            return random.choice(hpo_synonyms_secondary[term])
        else:
            return term
    else:
        return random.choice(invalid_secondary)

# --------------------------------------------------
# Disease codes (DO) 
# --------------------------------------------------
valid_doid_terms = [
    "DOID:9352",   # Diabetes mellitus type 2
    "DOID:14330",  # Hypertension
    "DOID:11476",  # Obesity
    "DOID:10534",  # Type 2 diabetes mellitus
    "DOID:9480",   # Dyslipidemia
]
invalid_doid_terms = [
    "DOID:XXXX",   # unmapped
    "DOID:9999999",
    "FAKE_DOID",
]

def random_doid_label():
    # ~80% valid, ~20% invalid
    if random.random() < 0.8:
        return random.choice(valid_doid_terms)
    else:
        return random.choice(invalid_doid_terms)

# --------------------------------------------------
# Observed features (array) => mix of valid/invalid HPO
# --------------------------------------------------
hpo_synonyms_all = dict(list(hpo_synonyms_primary.items()) + list(hpo_synonyms_secondary.items()))

def random_feature_array():
    length = np.random.randint(0, 4)  # 0-3 items
    if length == 0:
        return []
    arr = []
    for _ in range(length):
        # 70% valid
        if random.random() < 0.7:
            term = random.choice(valid_hpo_terms + valid_hpo_terms_secondary)
            # maybe use an ID or a synonym
            if random.random() < 0.5 and term in hpo_synonyms_all:
                arr.append(random.choice(hpo_synonyms_all[term]))
            else:
                arr.append(term)
        else:
            arr.append(random.choice(invalid_hpo_terms + invalid_secondary))
    return arr

# --------------------------------------------------
# Valid genome/hospital IDs
# --------------------------------------------------
valid_genome_ids = [f"GS_{i:05d}" for i in range(1, 2001)]
invalid_genome_ids = [f"GS_INVALID_{i}" for i in range(1, 101)]
valid_hospital_ids = [f"HID_{i:04d}" for i in range(1, 501)]
invalid_hospital_ids = [f"HID_BAD_{i}" for i in range(1, 51)]

# --------------------------------------------------
# Initialization
# --------------------------------------------------
np.random.seed(42)
random.seed(42)

# Numeric features
height = np.random.normal(170, 10, num_records)
weight = np.random.normal(70, 15, num_records)
cholesterol = np.random.normal(200, 40, num_records)
bp_systolic = np.random.normal(120, 15, num_records)
bp_diastolic = np.random.normal(80, 10, num_records)
glucose = np.random.normal(100, 20, num_records)
creatinine = np.random.normal(1.0, 0.3, num_records)

# Missingness in numeric
for arr in [height, weight, cholesterol, bp_systolic, bp_diastolic, glucose, creatinine]:
    arr[np.random.rand(num_records) < missing_rate] = np.nan

# Extreme cholesterol outliers
outlier_indices = np.random.choice(num_records, size=5, replace=False)
cholesterol[outlier_indices] = np.random.choice([1000, 2000, 3000], size=5)

# Primary phenotype
primary_pheno = [random_hpo_label_primary() for _ in range(num_records)]
for i in range(num_records):
    if random.random() < missing_rate:
        primary_pheno[i] = None

# Secondary phenotype
secondary_pheno = [random_hpo_label_secondary() for _ in range(num_records)]
for i in range(num_records):
    if random.random() < missing_rate:
        secondary_pheno[i] = None

# Disease code
disease_col = [random_doid_label() for _ in range(num_records)]
for i in range(num_records):
    if random.random() < missing_rate:
        disease_col[i] = None

# Observed features
feature_arrays = [random_feature_array() for _ in range(num_records)]
for i in range(num_records):
    if random.random() < missing_rate:
        feature_arrays[i] = None

# Date columns
start_date = datetime.today() - timedelta(days=365*10)
visit_dates = []
collection_datetimes = []
for i in range(num_records):
    # VisitDate
    if random.random() < missing_rate:
        visit_dates.append(None)
    else:
        if random.random() < 0.95:
            days_offset = np.random.randint(0, 365*10)
            visit_dates.append((start_date + timedelta(days=days_offset)).strftime('%Y-%m-%d'))
        else:
            visit_dates.append(random.choice(["NOT_A_DATE", "2023-13-40", "0000-00-00", "31/02/2021"]))

    # SampleCollectionDateTime
    if random.random() < missing_rate:
        collection_datetimes.append(None)
    else:
        if random.random() < 0.95:
            days_offset = np.random.randint(0, 365*10)
            seconds_offset = np.random.randint(0, 86400)
            valid_dt = start_date + timedelta(days=days_offset, seconds=seconds_offset)
            collection_datetimes.append(valid_dt.isoformat())
        else:
            collection_datetimes.append("INVALID_DATETIME_99")

# Genome/Hospital IDs
genome_ids = np.random.choice(valid_genome_ids + invalid_genome_ids, size=num_records)
hospital_ids = np.random.choice(valid_hospital_ids + invalid_hospital_ids, size=num_records)

genome_ids[np.random.rand(num_records) < missing_rate] = None
hospital_ids[np.random.rand(num_records) < missing_rate] = None

# Combine
df = pd.DataFrame({
    "SampleID": np.arange(1, num_records + 1),
    "Height_cm": height,
    "Weight_kg": weight,
    "Cholesterol_mgdl": cholesterol,
    "BP_systolic": bp_systolic,
    "BP_diastolic": bp_diastolic,
    "Glucose_mgdl": glucose,
    "Creatinine_mgdl": creatinine,
    "PrimaryPhenotype": primary_pheno,
    "SecondaryPhenotype": secondary_pheno,
    "DiseaseCode": disease_col,
    "ObservedFeatures": feature_arrays,
    "VisitDate": visit_dates,
    "SampleCollectionDateTime": collection_datetimes,
    "GenomeSampleID": genome_ids,
    "HospitalID": hospital_ids
})

# Duplicates & conflicts
num_duplicates = int(num_records * duplicate_rate)
df_duplicates = df.sample(num_duplicates, random_state=42).copy()

if num_duplicates > 0:
    # 1) unmapped phenotype
    df_duplicates.iloc[0, df_duplicates.columns.get_loc('PrimaryPhenotype')] = "CompletelyUnknownTerm"

    # 2) impossible numeric value
    if num_duplicates > 1:
        df_duplicates.iloc[1, df_duplicates.columns.get_loc('Height_cm')] = -999

    # 3) invalid disease code
    if num_duplicates > 2:
        df_duplicates.iloc[2, df_duplicates.columns.get_loc('DiseaseCode')] = "DOID:FAKE9999"

    # 4) entire feature array replaced with nonsense
    if num_duplicates > 3:
        df_duplicates.at[df_duplicates.index[3], 'ObservedFeatures'] = ["NotARealHPO", "HP:ZZZZZZZ"]

    # 5) invalid GenomeSampleID
    if num_duplicates > 4:
        df_duplicates.iloc[4, df_duplicates.columns.get_loc('GenomeSampleID')] = "GS_MISSING_REF"

    # 6) invalid HospitalID
    if num_duplicates > 5:
        df_duplicates.iloc[5, df_duplicates.columns.get_loc('HospitalID')] = "HID_BAD_REF"

# Shuffle
df = pd.concat([df, df_duplicates], ignore_index=True)
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

# Save
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df.to_csv(output_path, index=False)
print(f"Very complex, improved synthetic dataset saved to {output_path}.")

# ----------------------------------------------------------------
# Explanation of Notable Changes:
# ----------------------------------------------------------------
# 1) `SecondaryPhenotype` references real HPO terms or synonyms 
#    (e.g., “Fatigue”, “Ataxia”, “HP:0002013”, etc.), plus some invalid tags.
# 2) More synonyms to provide realistic textual variety in the data.
# 3) Additional invalid placeholders scattered throughout.
# 4) We still handle missingness, outliers, duplicates, etc., 
#    ensuring a thorough test of the pipeline.
