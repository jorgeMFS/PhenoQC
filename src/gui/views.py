from typing import List, Dict

QUALITY_OPTIONS = ["accuracy", "redundancy", "traceability", "timeliness", "all"]

def build_quality_metrics_widget(cfg: Dict) -> Dict:
    """Return widget configuration for quality metrics selection.

    Parameters
    ----------
    cfg : dict
        Current configuration dictionary.

    Returns
    -------
    dict
        Dictionary with available options and currently selected metrics.
    """
    selected = cfg.get("quality_metrics", []) if cfg else []
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
