from typing import List, Dict

from ..quality_metrics import QUALITY_METRIC_CHOICES

QUALITY_OPTIONS = QUALITY_METRIC_CHOICES + ["all"]

def build_quality_metrics_widget(cfg: Dict) -> Dict:
    """Return widget configuration for quality metrics selection.

    - Supports both list-style (e.g., ["accuracy", ...]) and dict-style
      (e.g., {"imputation_bias": {...}, "imputation_stability": {...}})
    - Filters unknown keys not in QUALITY_OPTIONS
    - Excludes diagnostics that are configured elsewhere in the UI (e.g.,
      imputation_stability), preventing Streamlit defaults from including
      values that are not present in the options list.
    """
    selected_raw = (cfg or {}).get("quality_metrics", [])
    # Normalize to a list of known metrics
    if isinstance(selected_raw, dict):
        # Take enabled metrics (or all present) that are part of QUALITY_METRIC_CHOICES
        enabled_keys = []
        for k, v in selected_raw.items():
            try:
                is_enabled = bool(v.get("enable", True)) if isinstance(v, dict) else bool(v)
            except Exception:
                is_enabled = True
            if is_enabled and k in QUALITY_METRIC_CHOICES:
                enabled_keys.append(k)
        selected = enabled_keys
    elif isinstance(selected_raw, list):
        selected = [m for m in selected_raw if m in QUALITY_METRIC_CHOICES]
    else:
        selected = []
    return {"options": QUALITY_OPTIONS, "selected": selected}

def apply_quality_metrics_selection(cfg: Dict, selection: List[str]) -> Dict:
    """Update configuration based on UI selection.

    If ``all`` is selected, every metric is enabled.
    """
    if selection is None:
        return cfg
    if "all" in selection:
        cfg["quality_metrics"] = [opt for opt in QUALITY_OPTIONS if opt != "all"]
    else:
        cfg["quality_metrics"] = selection
    return cfg
