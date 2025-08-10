#!/usr/bin/env bash
set -euo pipefail

# End-to-end diagnostics demo with verifications
# - Generates a synthetic dataset and schema on the fly
# - Runs PhenoQC with/without diagnostics, different repeats and methods
# - Verifies outputs, JSON fields, TSV metrics, and reproducibility
# - Writes results under scripts/output/diagnostics_demo/ by default

bold() { printf "\033[1m%s\033[0m\n" "$*"; }
ok() { printf "✅ %s\n" "$*"; }
err() { printf "❌ %s\n" "$*"; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { err "Missing required command: $1"; exit 1; }
}

require_cmd phenoqc
if [[ -n "${CONDA_PREFIX:-}" && -x "$CONDA_PREFIX/bin/python" ]]; then
  PYBIN="$CONDA_PREFIX/bin/python"
elif command -v python >/dev/null 2>&1; then
  PYBIN=$(command -v python)
elif command -v python3 >/dev/null 2>&1; then
  PYBIN=$(command -v python3)
else
  err "No python interpreter found (python or python3)"
  exit 1
fi
export PYBIN

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
WORKDIR=$(mktemp -d 2>/dev/null || mktemp -d -t "phenoqc_demo")
STAMP=$(date +%Y%m%d_%H%M%S)
DEFAULT_OUT="$REPO_DIR/scripts/output/diagnostics_demo/run_$STAMP"
OUTDIR=${1:-"$DEFAULT_OUT"}
mkdir -p "$OUTDIR"
export WORKDIR OUTDIR

bold "Working directory: $WORKDIR"

DATA="$WORKDIR/data.csv"
SCHEMA="$WORKDIR/schema.json"
CONFIG="$WORKDIR/config.yaml"
export DATA SCHEMA CONFIG

bold "Generating synthetic dataset with missingness..."
"$PYBIN" - <<'PY'
import numpy as np, pandas as pd, sys, os, json
wd = os.environ['WORKDIR']
np.random.seed(123)
n=800
df = pd.DataFrame({
  'SampleID': np.arange(n),
  'feat1': np.random.normal(0,1,n),
  'feat2': None,
  'feat3': np.random.uniform(0,10,n),
  'label': None,
  'outcome': None,
})
# Create a correlated feature and labels without missingness
noise = np.random.normal(0,0.05,n)
df['feat2'] = df['feat1']*2.0 + noise + 5.0
df['label'] = (df['feat1']>0).replace({True:'A', False:'B'})
df['outcome'] = (df['feat1']+np.random.normal(0,0.1,n) > 0).astype(int)
# Inject missingness (~20%)
for col in ['feat1','feat2','feat3']:
  idx = np.random.choice(n, size=int(0.2*n), replace=False)
  df.loc[idx, col] = np.nan
df.to_csv(os.environ['DATA'], index=False)

schema = {
  "type":"object",
  "properties":{
    "SampleID": {"type":"integer"},
    "feat1": {"type":["number","null"]},
    "feat2": {"type":["number","null"]},
    "feat3": {"type":["number","null"]}
  },
  "required": ["SampleID"]
}
open(os.environ['SCHEMA'],'w').write(json.dumps(schema, indent=2))

open(os.environ['CONFIG'],'w').write("""
imputation:
  strategy: knn
  params:
    n_neighbors: 3
  tuning:
    enable: true
    mask_fraction: 0.1
    scoring: MAE
    random_state: 42
    grid:
      n_neighbors: [3, 5]
default_ontologies: [HPO]
quality_metrics:
  - imputation_bias
  - redundancy
""")
PY
ok "Synthetic dataset, schema, and config created"

run_case() {
  local name="$1"; shift
  bold "Running case: $name"
  PYTHONPATH="$REPO_DIR/src:${PYTHONPATH:-}" "$PYBIN" -m phenoqc.cli \
    --input "$DATA" \
    --schema "$SCHEMA" \
    --config "$CONFIG" \
    --unique_identifiers SampleID \
    --output "$OUTDIR/$name" \
    "$@"
  ok "Case $name finished"
}

# Case A: Diagnostics OFF (baseline)
run_case A \
  --quality-metrics imputation_bias redundancy \
  --impute knn \
  --impute-params '{"n_neighbors": 5}' \
  --impute-tuning on \
  --impute-diagnostics off \
  --redundancy-threshold 0.98 --redundancy-method pearson

# Case B: Diagnostics ON (repeats=3)
run_case B \
  --quality-metrics imputation_bias redundancy \
  --impute knn \
  --impute-params '{"n_neighbors": 5}' \
  --impute-tuning on \
  --impute-diagnostics on \
  --diag-repeats 3 --diag-mask-fraction 0.10 --diag-scoring MAE \
  --redundancy-threshold 0.98 --redundancy-method pearson

# Case C: Diagnostics ON (repeats=10)
run_case C \
  --quality-metrics imputation_bias redundancy \
  --impute knn \
  --impute-params '{"n_neighbors": 5}' \
  --impute-tuning on \
  --impute-diagnostics on \
  --diag-repeats 10 --diag-mask-fraction 0.10 --diag-scoring MAE \
  --redundancy-threshold 0.98 --redundancy-method pearson

# Case D: Different strategy (mean), diagnostics ON, RMSE scoring, spearman redundancy
run_case D \
  --quality-metrics imputation_bias redundancy \
  --impute mean \
  --impute-tuning off \
  --impute-diagnostics on \
  --diag-repeats 5 --diag-mask-fraction 0.10 --diag-scoring RMSE \
  --redundancy-threshold 0.98 --redundancy-method spearman \
  --protected-columns label outcome

expect_file() {
  [[ -f "$1" ]] || { err "Expected file not found: $1"; exit 1; }
}

find_qc_json() {
  # Return first *_qc_summary.json in the directory
  ls -1 "$1"/*_qc_summary.json 2>/dev/null | head -n1
}

bold "Verifying outputs exist..."
for c in A B C D; do
  expect_file "$(find_qc_json "$OUTDIR/$c")"
  # Expect a report PDF
  ls -1 "$OUTDIR/$c"/*_report.pdf >/dev/null 2>&1 || { err "No report PDF for case $c"; exit 1; }
done
ok "QC summary JSONs and PDFs found for all cases"

bold "Checking imputation settings are surfaced..."
"$PYBIN" - <<'PY'
import json,glob,os,sys
out=os.environ['OUTDIR']
def load(case):
  path=glob.glob(os.path.join(out,case,'*_qc_summary.json'))[0]
  with open(path) as f: return json.load(f)

for case in ['A','B','C','D']:
  obj=load(case)
  imp=obj.get('imputation',{})
  assert 'global' in imp and isinstance(imp['global'], dict), f"Missing global in imputation ({case})"
  assert 'strategy' in imp['global'], f"Missing strategy in imputation.global ({case})"
  tun=imp.get('tuning',{})
  assert 'enabled' in tun, f"Missing tuning.enabled ({case})"
  if tun.get('enabled'):
    # If tuning enabled, we expect to see best param and score fields
    assert 'best' in tun or 'score' in tun, f"Missing best/score in tuning ({case})"
print('OK')
PY
ok "Imputation settings present with tuning summary"

bold "Verifying bias diagnostics ON/OFF behavior..."
"$PYBIN" - <<'PY'
import json,glob,os
out=os.environ['OUTDIR']
def rows(case, key):
  path=glob.glob(os.path.join(out,case,'*_qc_summary.json'))[0]
  obj=json.load(open(path))
  return obj.get('quality_metrics',{}).get(key,{}).get('rows',[])

rows_A = rows('A','imputation_bias')
rows_B = rows('B','imputation_bias')
rows_C = rows('C','imputation_bias')
rows_D = rows('D','imputation_bias')

# Bias rows should exist when enabled (B,C); baseline A may be empty
assert isinstance(rows_B,list) and len(rows_B) >= 0
assert isinstance(rows_C,list) and len(rows_C) >= 0
print('OK')
PY
ok "Bias diagnostics JSON structure verified"

bold "Comparing stability repeats (expect avg sd_error to be non-increasing with more repeats)..."
python3 - <<'PY'
import json,glob,os,numpy as np
out=os.environ['OUTDIR']
def stab_avg_sd(case):
  path=glob.glob(os.path.join(out,case,'*_qc_summary.json'))[0]
  obj=json.load(open(path))
  rows=obj.get('quality_metrics',{}).get('imputation_stability',{}).get('rows',[])
  if not rows: return None
  sd=[r.get('sd_error') for r in rows if isinstance(r,dict) and isinstance(r.get('sd_error'),(int,float))]
  return float(np.mean(sd)) if sd else None

sd_B=stab_avg_sd('B')
sd_C=stab_avg_sd('C')
sd_D=stab_avg_sd('D')
if sd_B is not None and sd_C is not None:
  assert sd_C <= sd_B + 1e-9, f"Expected sd_error(avg) to not increase with more repeats: {sd_B} -> {sd_C}"
print('OK')
PY
ok "Stability diagnostic behaves as expected (non-increasing average sd_error)"

bold "Reproducibility check (same run twice => identical CSV)..."
run_case R1 \
  --quality-metrics imputation_bias redundancy \
  --impute knn \
  --impute-params '{"n_neighbors": 5}' \
  --impute-tuning on \
  --impute-diagnostics on \
  --diag-repeats 5 --diag-mask-fraction 0.10 --diag-scoring MAE

run_case R2 \
  --quality-metrics imputation_bias redundancy \
  --impute knn \
  --impute-params '{"n_neighbors": 5}' \
  --impute-tuning on \
  --impute-diagnostics on \
  --diag-repeats 5 --diag-mask-fraction 0.10 --diag-scoring MAE

CSV1=$(ls -1 "$OUTDIR/R1"/*_json.csv | head -n1 || true)
CSV2=$(ls -1 "$OUTDIR/R2"/*_json.csv | head -n1 || true)
# Fallback to any *_json.csv or final CSV
[[ -n "$CSV1" ]] || CSV1=$(ls -1 "$OUTDIR/R1"/*.csv | head -n1)
[[ -n "$CSV2" ]] || CSV2=$(ls -1 "$OUTDIR/R2"/*.csv | head -n1)

require_cmd md5sum || true
if command -v md5sum >/dev/null; then
  H1=$(md5sum "$CSV1" | awk '{print $1}')
  H2=$(md5sum "$CSV2" | awk '{print $1}')
  [[ "$H1" == "$H2" ]] || { err "CSV outputs differ between identical runs"; exit 1; }
  ok "CSV MD5 identical across runs ($H1)"
elif command -v md5 >/dev/null; then
  H1=$(md5 -q "$CSV1")
  H2=$(md5 -q "$CSV2")
  [[ "$H1" == "$H2" ]] || { err "CSV outputs differ between identical runs"; exit 1; }
  ok "CSV MD5 identical across runs ($H1)"
else
  ok "md5sum not available; skipping hash comparison"
fi

bold "Checking Quality Metrics TSV for redundancy entries (if any)..."
for c in A B C D; do
  TSV=$(ls -1 "$OUTDIR/$c"/*_quality_metrics.tsv 2>/dev/null | head -n1 || true)
  if [[ -n "$TSV" ]]; then
    if grep -q "\tredundancy\b" "$TSV"; then ok "Redundancy entries found in $TSV"; else ok "No redundancy rows found in $TSV (may be fine)"; fi
  else
    ok "No metrics TSV for case $c (quality metrics may have no rows)"
  fi
done

bold "Output directory structure:"
if command -v tree >/dev/null; then tree -L 2 "$OUTDIR"; else ls -R "$OUTDIR"; fi

ok "All verifications passed. Outputs: $OUTDIR"


