import pytest
from phenoqc.gui.views import build_quality_metrics_widget, apply_quality_metrics_selection


def test_build_quality_metrics_widget_options_and_selection():
    cfg = {"quality_metrics": ["imputation_bias", "redundancy"]}
    widget = build_quality_metrics_widget(cfg)
    assert "options" in widget and "selected" in widget
    assert "all" in widget["options"]
    assert set(cfg["quality_metrics"]).issubset(set(widget["options"]))
    assert widget["selected"] == cfg["quality_metrics"]


def test_apply_quality_metrics_selection_all_enables_all():
    cfg = {}
    updated = apply_quality_metrics_selection(cfg, ["all"]) 
    assert "all" not in updated["quality_metrics"]
    assert len(updated["quality_metrics"]) >= 1


def test_apply_quality_metrics_selection_specific_list():
    cfg = {}
    sel = ["accuracy", "redundancy"]
    updated = apply_quality_metrics_selection(cfg, sel)
    assert updated["quality_metrics"] == sel


