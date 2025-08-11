#!/usr/bin/env python3

"""
generate_synthetic_ontology_data.py

Purpose:
  - Reads 'config.yaml' which defines 3 ontologies: HPO, DO, MPO.
  - Loads them via pronto (remote or local).
  - Generates ~N synthetic rows with numeric columns, 
    plus 4 ontology-based text columns:
      1) PrimaryPhenotype     -> from HPO
      2) SecondaryPhenotype   -> from HPO
      3) TertiaryPhenotype    -> from MPO
      4) DiseaseCode          -> from DO
    Possibly inserts missing data, invalid text, duplicates, date/time columns, etc.
  - Saves two CSV files:
      - synthetic_multi_data.csv         (the main phenotypic dataset)
      - synthetic_multi_ground_truth.csv (maps SampleID -> True[HPO/DO/MPO]_ID)

Example usage:
  python generate_synthetic_ontology_data.py \
    --config ./config/config.yaml \
    --num_records 3000 \
    --output_prefix ./output/synthetic_multi

Then you'll have:
  - ./synthetic_multi_data.csv
  - ./synthetic_multi_ground_truth.csv

Subsequently, run PhenoQC with:
    phenoqc \
            --input ./output/synthetic_multi_data.csv \
            --output ./output/reports \
            --schema ./config/schema.json \
            --config ./config/config.yaml \
            --unique_identifiers SampleID \
            --ontologies HPO DO MPO \
            --phenotype_columns '{"PrimaryPhenotype": ["HPO"], "DiseaseCode": ["DO"], "TertiaryPhenotype": ["MPO"]}'


Then check the resulting columns [HPO_ID, DO_ID, MPO_ID] vs. ground truth columns [TruePrimaryHPO_ID, TrueDiseaseDO_ID, TrueTertiaryMPO_ID], etc.
"""

import argparse
import os
import random
import yaml
import pandas as pd
import numpy as np
import pronto
from datetime import datetime, timedelta

# --------------------------------------------------------------------
# Utility to load config
# --------------------------------------------------------------------
def load_config_yaml(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

# --------------------------------------------------------------------
# Utility to load an ontology via pronto (URL or local)
# --------------------------------------------------------------------
def load_ontology(ont_config):
    """
    ont_config example:
      {
         "name": "Human Phenotype Ontology",
         "source": "url",
         "url": "http://purl.obolibrary.org/obo/hp.obo",
         "format": "obo"
      }
    """
    import requests
    import tempfile

    source = ont_config.get('source', 'local').lower()
    file_format = ont_config.get('format', 'obo')

    if source == 'local':
        onto_path = ont_config['file']
        if not os.path.exists(onto_path):
            raise FileNotFoundError(f"[ERROR] Local ontology file not found: {onto_path}")
        print(f"[INFO] Loading ontology from local file: {onto_path}")
        onto = pronto.Ontology(onto_path)
    elif source == 'url':
        url = ont_config['url']
        # Try local fallbacks before downloading again
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            basename = os.path.basename(url) if url else None
            candidates = []
            if basename:
                candidates.append(os.path.join(project_root, 'ontologies', basename))
                # Also try uppercase variant (e.g., HPO.obo)
                candidates.append(os.path.join(project_root, 'ontologies', basename.upper()))
            # Common known local files
            candidates.extend([
                os.path.join(project_root, 'ontologies', 'hp.obo'),
                os.path.join(project_root, 'ontologies', 'doid.obo'),
                os.path.join(project_root, 'ontologies', 'mp.obo'),
            ])
            # User cache location used by the main app
            home_cache_dir = os.path.join(os.path.expanduser('~'), '.phenoqc', 'ontologies')
            if basename:
                candidates.append(os.path.join(home_cache_dir, basename))
                candidates.append(os.path.join(home_cache_dir, basename.upper() if basename else ''))
            for cand in candidates:
                if cand and os.path.exists(cand):
                    print(f"[INFO] Using cached/local ontology: {cand}")
                    return pronto.Ontology(cand)
        except Exception:
            # Silently continue to download
            pass

        print(f"[INFO] Downloading ontology from URL: {url}")
        resp = requests.get(url)
        if resp.status_code != 200:
            raise Exception(f"Failed to download from {url}, status code={resp.status_code}")
        # Write to temp with explicit UTF-8 encoding
        tf = tempfile.NamedTemporaryFile(delete=False, suffix='.'+file_format, mode='w', encoding='utf-8')
        tf.write(resp.text)  # Use text instead of content for proper encoding
        tf.flush()
        tf.close()
        onto = pronto.Ontology(tf.name)
        os.remove(tf.name)
    else:
        raise ValueError(f"[ERROR] Unknown source '{source}' in ontology config.")
    return onto

# --------------------------------------------------------------------
# Utility to pick random valid terms from an ontology
# --------------------------------------------------------------------
def pick_valid_terms(onto, num_samples=1000, seed=42):
    random.seed(seed)
    all_terms = []
    for term in onto.terms():
        if not term.name or term.obsolete:
            continue
        all_terms.append(term)

    if len(all_terms) == 0:
        raise RuntimeError("[ERROR] No valid terms found in this ontology.")

    if num_samples >= len(all_terms):
        chosen = all_terms
    else:
        chosen = random.sample(all_terms, num_samples)
    return chosen

# --------------------------------------------------------------------
# Utility to produce real-synonym or official text from a term
# with possible corruption
# --------------------------------------------------------------------
def randomize_text_from_term(term, corruption_chance=0.1):
    synonyms = [syn.description for syn in term.synonyms if syn.description]
    possible_texts = [term.name] + synonyms
    chosen_text = random.choice(possible_texts).strip()

    if random.random() < corruption_chance:
        # e.g. add random trailing substring
        chosen_text += random.choice(["(typo)", "???", " RubbishEnd", " (typo2)"])
    return chosen_text

# --------------------------------------------------------------------
# Utility to produce entirely invalid text
# --------------------------------------------------------------------
def maybe_insert_invalid_term():
    candidates = ["NotARealTerm", "ZZZZ:9999999", "PhenotypeJunk", "InvalidTerm42", ""]
    return random.choice(candidates)

# --------------------------------------------------------------------
# Main script
# --------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate synthetic phenotypic dataset referencing HPO/DO/MPO.")
    parser.add_argument("--config", required=True, help="Path to config.yaml.")
    parser.add_argument("--num_records", type=int, default=3000, help="Number of synthetic rows.")
    parser.add_argument("--missing_rate", type=float, default=0.1, help="Fraction of missingness in numeric/text columns.")
    parser.add_argument("--duplicate_rate", type=float, default=0.05, help="Fraction of duplicates to produce.")
    parser.add_argument("--hpo_samples", type=int, default=3000, help="How many HPO terms to sample from the ontology.")
    parser.add_argument("--do_samples", type=int, default=3000, help="How many DO terms to sample from the ontology.")
    parser.add_argument("--mpo_samples", type=int, default=3000, help="How many MPO terms to sample from the ontology.")
    parser.add_argument("--invalid_rate", type=float, default=0.1, 
                        help="Chance that a row's text is replaced with nonsense for an ontology-based column.")
    parser.add_argument("--output_prefix", default="synthetic_multi", help="Prefix for output CSV files.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    args = parser.parse_args()

    random.seed(args.seed)
    np.random.seed(args.seed)

    # 1) Load config + find HPO/DO/MPO
    cfg = load_config_yaml(args.config)
    ont_cfgs = cfg.get('ontologies', {})
    hpo_cfg = ont_cfgs.get('HPO')
    do_cfg  = ont_cfgs.get('DO')
    mpo_cfg = ont_cfgs.get('MPO')
    if not hpo_cfg or not do_cfg or not mpo_cfg:
        raise ValueError("[ERROR] config.yaml must have HPO, DO, MPO definitions in 'ontologies'.")

    # 2) Load the three ontologies
    print("[INFO] Loading HPO ...")
    onto_hpo = load_ontology(hpo_cfg)
    print("[INFO] Loading DO ...")
    onto_do  = load_ontology(do_cfg)
    print("[INFO] Loading MPO ...")
    onto_mpo = load_ontology(mpo_cfg)

    # 3) Sample some terms from each
    hpo_terms = pick_valid_terms(onto_hpo, num_samples=args.hpo_samples, seed=args.seed)
    do_terms  = pick_valid_terms(onto_do,  num_samples=args.do_samples,  seed=args.seed+1)
    mpo_terms = pick_valid_terms(onto_mpo, num_samples=args.mpo_samples, seed=args.seed+2)

    # Convert to lists for easy random.choice
    hpo_terms_list = list(hpo_terms)
    do_terms_list  = list(do_terms)
    mpo_terms_list = list(mpo_terms)

    # 4) Numeric columns
    num_records = args.num_records
    missing_rate = args.missing_rate
    duplicate_rate = args.duplicate_rate

    height = np.random.normal(170, 10, num_records)
    weight = np.random.normal(70, 15, num_records)
    chol   = np.random.normal(200, 40, num_records)
    bp_syst = np.random.normal(120, 15, num_records)
    bp_diast= np.random.normal(80, 10, num_records)
    glucose= np.random.normal(100, 20, num_records)
    creatinine= np.random.normal(1.0, 0.3, num_records)

    for arr in [height, weight, chol, bp_syst, bp_diast, glucose, creatinine]:
        arr[np.random.rand(num_records) < missing_rate] = np.nan

    # Outliers for cholesterol
    outlier_indices = np.random.choice(num_records, size=5, replace=False)
    chol[outlier_indices] = np.random.choice([1000, 2000, 3000], size=5)

    # 5) Build the 4 text columns + ground truth
    #    Primary/Secondary => HPO, Tertiary => MPO, Disease => DO
    #    We'll store them in lists + parallel truth lists
    primary_list, primary_truth = [], []
    secondary_list, secondary_truth = [], []
    tertiary_list, tertiary_truth = [], []
    disease_list, disease_truth = [], []

    def random_hpo_text():
        if random.random() < args.invalid_rate:
            return maybe_insert_invalid_term(), ""
        else:
            term = random.choice(hpo_terms_list)
            txt  = randomize_text_from_term(term, corruption_chance=0.1)
            return txt, term.id

    def random_do_text():
        if random.random() < args.invalid_rate:
            return maybe_insert_invalid_term(), ""
        else:
            term = random.choice(do_terms_list)
            txt  = randomize_text_from_term(term, corruption_chance=0.1)
            return txt, term.id

    def random_mpo_text():
        if random.random() < args.invalid_rate:
            return maybe_insert_invalid_term(), ""
        else:
            term = random.choice(mpo_terms_list)
            txt  = randomize_text_from_term(term, corruption_chance=0.1)
            return txt, term.id

    for i in range(num_records):
        # Primary
        p_txt, p_id = random_hpo_text()
        # missing?
        if random.random() < missing_rate:
            p_txt = None
            p_id  = ""
        primary_list.append(p_txt)
        primary_truth.append(p_id)

        # Secondary
        s_txt, s_id = random_hpo_text()
        if random.random() < missing_rate:
            s_txt = None
            s_id  = ""
        secondary_list.append(s_txt)
        secondary_truth.append(s_id)

        # Tertiary (MPO)
        t_txt, t_id = random_mpo_text()
        if random.random() < missing_rate:
            t_txt = None
            t_id  = ""
        tertiary_list.append(t_txt)
        tertiary_truth.append(t_id)

        # Disease (DO)
        d_txt, d_id = random_do_text()
        if random.random() < missing_rate:
            d_txt = None
            d_id  = ""
        disease_list.append(d_txt)
        disease_truth.append(d_id)

    # 6) ObservedFeatures array (like your original approach, mostly HPO-ish strings)
    def random_feature_array():
        length = np.random.randint(0, 4)  # 0..3 items
        if length == 0:
            return []
        arr = []
        for _ in range(length):
            if random.random() < 0.7:
                # pick real HPO
                term = random.choice(hpo_terms_list)
                arr.append(randomize_text_from_term(term, corruption_chance=0.05))
            else:
                arr.append(maybe_insert_invalid_term())
        return arr

    obs_features = [random_feature_array() for _ in range(num_records)]
    for i in range(num_records):
        if random.random() < missing_rate:
            obs_features[i] = None

    # 7) Date columns
    start_date = datetime.today() - timedelta(days=365*10)
    visit_dates = []
    collection_datetimes = []
    for i in range(num_records):
        # VisitDate
        if random.random() < missing_rate:
            visit_dates.append(None)
        else:
            if random.random() < 0.95:
                offset = np.random.randint(0, 365*10)
                dt_ = (start_date + timedelta(days=offset)).strftime('%Y-%m-%d')
                visit_dates.append(dt_)
            else:
                # invalid date
                visit_dates.append(random.choice(["NOT_A_DATE", "2023-13-40", "0000-00-00", "31/02/2021"]))

        # SampleCollectionDateTime
        if random.random() < missing_rate:
            collection_datetimes.append(None)
        else:
            if random.random() < 0.95:
                offset = np.random.randint(0, 365*10)
                sec_offset = np.random.randint(0, 86400)
                dt_val = start_date + timedelta(days=offset, seconds=sec_offset)
                collection_datetimes.append(dt_val.isoformat())
            else:
                collection_datetimes.append("INVALID_DATETIME_99")

    # 8) Genome/Hospital IDs (with validity)
    valid_gs = [f"GS_{i:05d}" for i in range(1,2001)]
    invalid_gs= [f"GS_INVALID_{i}" for i in range(1,101)]
    valid_hid = [f"HID_{i:04d}" for i in range(1,501)]
    invalid_hid= [f"HID_BAD_{i}" for i in range(1,51)]
    all_gs = valid_gs + invalid_gs
    all_hid= valid_hid+ invalid_hid
    genome_ids = np.random.choice(all_gs, size=num_records)
    hospital_ids= np.random.choice(all_hid,size=num_records)

    for i in range(num_records):
        if random.random() < missing_rate:
            genome_ids[i] = None
        if random.random() < missing_rate:
            hospital_ids[i] = None

    # 9) Combine into a DataFrame
    df = pd.DataFrame({
        "SampleID": np.arange(1, num_records + 1),
        "Height_cm": height,
        "Weight_kg": weight,
        "Cholesterol_mgdl": chol,
        "BP_systolic": bp_syst,
        "BP_diastolic": bp_diast,
        "Glucose_mgdl": glucose,
        "Creatinine_mgdl": creatinine,
        "PrimaryPhenotype": primary_list,
        "SecondaryPhenotype": secondary_list,
        "TertiaryPhenotype": tertiary_list,   # from MPO
        "DiseaseCode": disease_list,          # from DO
        "ObservedFeatures": obs_features,
        "VisitDate": visit_dates,
        "SampleCollectionDateTime": collection_datetimes,
        "GenomeSampleID": genome_ids,
        "HospitalID": hospital_ids
    })

    # 10) Duplicates & conflicts
    num_duplicates = int(num_records * duplicate_rate)
    if num_duplicates > 0:
        df_dups = df.sample(num_duplicates, random_state=args.seed).copy()
        modifications = [
            (1, 'PrimaryPhenotype', "CompletelyUnknownTerm"),
            (2, 'Height_cm', -999),
            (3, 'DiseaseCode', "DOID:FAKE9999"),
            (4, 'ObservedFeatures', ["NotARealHPO", "HP:ZZZZZZZ"]),
            (5, 'GenomeSampleID', "GS_MISSING_REF"),
            (6, 'HospitalID', "HID_BAD_REF")
        ]
        
        for idx, (condition, column, value) in enumerate(modifications):
            if idx < len(df_dups):
                if column == 'ObservedFeatures':
                    df_dups.at[df_dups.index[idx], column] = value
                else:
                    df_dups.at[df_dups.index[idx], column] = value

        # Concat + shuffle
        df = pd.concat([df, df_dups], ignore_index=True)

    df = df.sample(frac=1, random_state=args.seed).reset_index(drop=True)

    # 11) Build ground-truth DataFrame
    # We store: [SampleID, TruePrimaryHPO_ID, TrueSecondaryHPO_ID, TrueTertiaryMPO_ID, TrueDiseaseDO_ID]
    # But note we shuffled df after creation. We'll handle that by index alignment:
    df['TempOrigID'] = range(len(df))  # store row index pre-shuffle if needed

    ground_truth = pd.DataFrame({
        "SampleID": np.arange(1, num_records + 1),
        "TempOrigID": np.arange(num_records),
        "TruePrimaryHPO_ID": primary_truth,
        "TrueSecondaryHPO_ID": secondary_truth,
        "TrueTertiaryMPO_ID": tertiary_truth,
        "TrueDiseaseDO_ID": disease_truth
    })

    # Because we appended duplicates, the ground_truth has only the *original* row count.
    # For duplicates, we can treat them as if they have no ground-truth row. 
    # Or we can replicate them. We'll do the simpler approach: replicate them if you want
    # ground truth for duplicates. Let's replicate them here:
    # But we only have primary_truth arrays of length num_records (no duplicates).
    # So let's store an empty or blank for duplicates. 
    # We'll do something simpler: the duplicates won't have ground truth, or we can leave them blank.
    # For each appended duplicate row, we store blank IDs in ground truth. 
    # The final approach is that we have N original rows with ground truth, plus duplicates with no truth. 
    # We'll handle that by merges on row index. 
    # We'll keep "TempOrigID" in the final df for merges.

    # For duplicates, "TempOrigID" doesn't exist, let's set them to -1
    # to indicate no ground truth
    # We can track which ones are appended. 
    # Actually, an easy approach is to do:
    n_original = num_records
    df.loc[n_original:, 'TempOrigID'] = -1

    # Then when we merge, those duplicates won't find a match => no ground truth
    # Let's do it.

    # 12) Save final CSV
    data_out = f"{args.output_prefix}_data.csv"
    df.to_csv(data_out, index=False)
    print(f"[INFO] Synthetic dataset saved to {data_out} (shape={df.shape})")

    # 13) Save ground truth
    # ground_truth has shape=(num_records, 5). We'll store it with TempOrigID as well.
    truth_out = f"{args.output_prefix}_ground_truth.csv"
    ground_truth.to_csv(truth_out, index=False)
    print(f"[INFO] Ground truth saved to {truth_out} (shape={ground_truth.shape})")
    print("[INFO] Done.")

if __name__ == "__main__":
    main()
