import os
import hashlib
import inspect
import logging
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from PIL import Image as PILImage

# ``reportlab`` internally calls ``hashlib.md5(usedforsecurity=False)``,
# but Python versions prior to 3.9 do not accept the ``usedforsecurity``
# keyword argument.  This causes a ``TypeError`` on those interpreters
# (notably Python 3.8 used in our CI).  To maintain compatibility we
# shim ``hashlib.md5`` so that it silently ignores the argument when the
# runtime does not support it.
_hashlib_md5 = hashlib.md5
if 'usedforsecurity' not in inspect.signature(_hashlib_md5).parameters:
    def _md5_compat(*args, **kwargs):
        kwargs.pop('usedforsecurity', None)
        return _hashlib_md5(*args, **kwargs)
    hashlib.md5 = _md5_compat

from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Image,
    Table,
    KeepTogether,
    HRFlowable,
    PageBreak,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch

from typing import Optional, Dict


def generate_qc_report(
    validation_results,
    missing_data,
    flagged_records_count,
    mapping_success_rates,
    visualization_images,
    impute_strategy,
    quality_scores,
    output_path_or_buffer,
    report_format='pdf',
    file_identifier=None,
    class_distribution=None,
    imputation_summary: Optional[Dict] = None,
    bias_diagnostics: Optional[pd.DataFrame] = None,
    bias_thresholds: Optional[Dict] = None,
    stability_diagnostics: Optional[pd.DataFrame] = None,
    quality_metrics_enabled: Optional[object] = None,
):
    """
    Generates a quality control report (PDF or Markdown).
    No changes to other files are required.
    """
    if report_format == 'pdf':
        styles = getSampleStyleSheet()
        # Compact style for table cells
        table_cell_style = ParagraphStyle(
            name="TableCell",
            parent=styles['BodyText'],
            fontSize=7,
            leading=9,
            spaceAfter=0,
            spaceBefore=0,
        )
        # Aggressive wrapping for long tokens (e.g., IDs without spaces)
        table_cell_style.wordWrap = 'CJK'

        # Header style to guarantee white text regardless of table styles
        table_header_style = ParagraphStyle(
            name="TableHeader",
            parent=table_cell_style,
            fontName='Helvetica-Bold',
            fontSize=8,
            leading=10,
            textColor=colors.white,
        )

        # Use landscape layout to provide more horizontal space
        page_size = landscape(letter)
        left_margin = right_margin = top_margin = bottom_margin = 36  # 0.5 inch
        available_width = page_size[0] - left_margin - right_margin
        available_height = page_size[1] - top_margin - bottom_margin

        story = []

        # Spacing constants
        SPACING_S = 8
        SPACING_M = 14
        SPACING_L = 22

        # Helpers
        def hr():
            return HRFlowable(width="100%", thickness=0.6, color=colors.HexColor('#BDC3C7'))

        section_header_style = ParagraphStyle(
            name="SectionHeader",
            parent=styles['Heading1'],
            fontSize=16,
            leading=20,
            spaceAfter=6,
        )
        subsection_header_style = ParagraphStyle(
            name="SubSectionHeader",
            parent=styles['Heading2'],
            fontSize=13,
            leading=16,
            spaceAfter=4,
        )

        # Cover / Title
        story.append(Paragraph("PhenoQC Quality Control Report", styles['Title']))
        if file_identifier:
            story.append(Spacer(1, 4))
            story.append(Paragraph(f"<b>Source file:</b> {file_identifier}", styles['Normal']))
        story.append(Spacer(1, SPACING_M))
        story.append(hr())
        story.append(Spacer(1, SPACING_M))

        # Summary section (kept together where feasible to avoid splitting across pages)
        summary_block = []
        summary_block.append(Paragraph("Summary", section_header_style))
        # Imputation Strategy + Quality Scores as label/value pairs (no header row)
        strategy_display = "(No Imputation Strategy)" if impute_strategy is None else impute_strategy.capitalize()
        label_style = ParagraphStyle(
            name="LabelCell",
            parent=table_cell_style,
            fontName='Helvetica-Bold',
            textColor=colors.black,
        )
        scores_items = [[Paragraph("Imputation Strategy", label_style), Paragraph(strategy_display, table_cell_style)]]
        # If available, include a concise imputation tuning summary
        # Expectation: quality_scores may be augmented elsewhere; keep resilient here
        for score_name, score_value in quality_scores.items():
            scores_items.append([
                Paragraph(score_name, label_style),
                Paragraph(f"{score_value:.2f}%", table_cell_style),
            ])
        scores_table = Table(
            scores_items,
            colWidths=[available_width * 0.45, available_width * 0.55],
            hAlign='LEFT',
        )
        scores_table.setStyle([
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#B0B7BF')),
            ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.whitesmoke, colors.HexColor('#ECF0F1')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ])
        summary_block.append(scores_table)
        summary_block.append(Spacer(1, SPACING_L))

        # Imputation settings (if provided)
        if isinstance(imputation_summary, dict) and imputation_summary:
            summary_block.append(Paragraph("Imputation Settings", subsection_header_style))
            rows = []
            global_cfg = imputation_summary.get('global', {})
            if global_cfg:
                rows.append([Paragraph("Global Strategy", label_style), Paragraph(str(global_cfg.get('strategy')), table_cell_style)])
                rows.append([Paragraph("Global Params", label_style), Paragraph(str(global_cfg.get('params')), table_cell_style)])
            tuning = imputation_summary.get('tuning', {})
            if tuning:
                rows.append([Paragraph("Tuning Enabled", label_style), Paragraph(str(tuning.get('enabled')), table_cell_style)])
                if 'best' in tuning:
                    rows.append([Paragraph("Best Params", label_style), Paragraph(str(tuning.get('best')), table_cell_style)])
                if 'score' in tuning and 'metric' in tuning:
                    rows.append([Paragraph("Tuning Score", label_style), Paragraph(f"{tuning['score']:.4f} ({tuning['metric']})", table_cell_style)])
            if rows:
                imp_table = Table(rows, colWidths=[available_width * 0.35, available_width * 0.65])
                imp_table.setStyle([
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#B0B7BF')),
                    ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.whitesmoke, colors.HexColor('#ECF0F1')]),
                ])
                summary_block.append(imp_table)
                summary_block.append(Spacer(1, SPACING_L))

        # Heuristic: keep together only if block is small enough; otherwise allow normal flow
        if len(summary_block) <= 6:
            story.append(KeepTogether(summary_block))
        else:
            story.extend(summary_block)

        # Data Quality Scores
        story.append(Paragraph("Data Quality Scores:", styles['Heading2']))
        for score_name, score_value in quality_scores.items():
            story.append(Paragraph(f"<b>{score_name}:</b> {score_value:.2f}%", styles['Normal']))
        story.append(Spacer(1, 12))

        # Schema Validation Results
        story.append(Paragraph("Schema Validation Results", subsection_header_style))
        quality_metric_keys = {"Accuracy Issues", "Redundancy Issues", "Traceability Issues", "Timeliness Issues"}
        for key, value in validation_results.items():
            # Skip quality metric entries here; they will be rendered in a dedicated section below
            if key in quality_metric_keys:
                continue
            if isinstance(value, pd.DataFrame):
                if not value.empty:
                    story.append(Paragraph(
                        f"<b>{key}:</b> {len(value)} issues found.",
                        styles['Normal']
                    ))
                else:
                    story.append(Paragraph(
                        f"<b>{key}:</b> No issues found.",
                        styles['Normal']
                    ))
            else:
                story.append(Paragraph(f"<b>{key}:</b> {value}", styles['Normal']))
        story.append(Spacer(1, SPACING_L))
        story.append(hr())
        story.append(Spacer(1, SPACING_L))

        # Helper: build a styled, wrapped table from a DataFrame that fits the page
        def build_dataframe_table(df: pd.DataFrame, title: str, max_rows: int = 50):
            flowables = []
            n_total = len(df)
            if n_total == 0:
                flowables.append(Paragraph(f"<b>{title}:</b> No issues found.", styles['Normal']))
                return flowables

            flowables.append(Paragraph(f"<b>{title}:</b> {n_total} issues found.", styles['Normal']))

            # Truncate to a reasonable number of rows for readability
            truncated = False
            if n_total > max_rows:
                df_to_show = df.head(max_rows).copy()
                truncated = True
            else:
                df_to_show = df.copy()

            # Convert to string and wrap each cell; headers use white text style
            headers = [Paragraph(str(h), table_header_style) for h in df_to_show.columns]
            body = []
            for _, row in df_to_show.iterrows():
                body.append([
                    Paragraph(str(val), table_cell_style) for val in row.tolist()
                ])

            # Compute column widths proportionally to content length, within bounds, fitting available width
            num_cols = max(1, len(headers))
            max_col_width = 2.3 * inch
            min_col_width = 0.8 * inch
            # Estimate per-column weight using header and 90th percentile of body length
            weights = []
            for col in df_to_show.columns:
                header_len = len(str(col))
                try:
                    q90 = int(df_to_show[col].astype(str).str.len().quantile(0.9))
                except Exception:
                    q90 = header_len
                weights.append(max(header_len, q90, 1))
            total_weight = float(sum(weights)) or float(num_cols)
            raw_widths = [max(min_col_width, min(max_col_width, (w / total_weight) * available_width)) for w in weights]
            # Normalize if we exceed available width due to min/max clamping
            scale = min(1.0, available_width / sum(raw_widths))
            col_widths = [w * scale for w in raw_widths]

            tbl = Table([headers] + body, colWidths=col_widths, repeatRows=1)
            tbl.setStyle([
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 7),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor('#ECF0F1')]),
                ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#B0B7BF')),
                ('LINEBELOW', (0, 0), (-1, 0), 0.5, colors.HexColor('#7F8C8D')),
                ('BOX', (0, 0), (-1, -1), 0.25, colors.HexColor('#B0B7BF')),
                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                ('TOPPADDING', (0, 0), (-1, -1), 2),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
            ])

            flowables.append(tbl)
            if truncated:
                flowables.append(Spacer(1, 6))
                flowables.append(Paragraph(
                    f"Showing first {max_rows} rows out of {n_total}.", styles['Italic']
                ))
            # Add extra space after each table block to separate metrics clearly
            flowables.append(Spacer(1, 18))
            return flowables

        # Additional Quality Dimensions (styled tables) - only if explicitly enabled
        enabled_ids = set()
        if isinstance(quality_metrics_enabled, list):
            enabled_ids = {str(m).lower() for m in quality_metrics_enabled}
        elif isinstance(quality_metrics_enabled, dict):
            # dictionary style not typically used for these metrics; treat any truthy flag as enabled
            for mid in ["accuracy", "redundancy", "traceability", "timeliness"]:
                try:
                    if quality_metrics_enabled.get(mid):
                        enabled_ids.add(mid)
                except Exception:
                    pass
        if enabled_ids:
            story.append(Paragraph("Additional Quality Dimensions", section_header_style))
            id_to_key = {
                "accuracy": "Accuracy Issues",
                "redundancy": "Redundancy Issues",
                "traceability": "Traceability Issues",
                "timeliness": "Timeliness Issues",
            }
            for metric_id in ["accuracy", "redundancy", "traceability", "timeliness"]:
                if metric_id not in enabled_ids:
                    continue
                metric = id_to_key[metric_id]
                df_metric = validation_results.get(metric, pd.DataFrame())
                if isinstance(df_metric, pd.DataFrame):
                    block = build_dataframe_table(df_metric, metric, max_rows=50)
                    story.append(KeepTogether(block))
                else:
                    story.append(Paragraph(f"<b>{metric}:</b> No issues found.", styles['Normal']))
            story.append(Spacer(1, SPACING_L))
            story.append(hr())
            story.append(Spacer(1, SPACING_L))

        # Class Distribution (optional)
        if class_distribution:
            story.append(Paragraph("Class Distribution", section_header_style))
            if isinstance(class_distribution, dict):
                counts = class_distribution.get('counts', {})
                proportions = class_distribution.get('proportions', {})
                warn_threshold = class_distribution.get('warn_threshold', 0.10)
                warning = class_distribution.get('warning', False)
            else:
                counts = getattr(class_distribution, 'counts', {})
                proportions = getattr(class_distribution, 'proportions', {})
                warn_threshold = getattr(class_distribution, 'warn_threshold', 0.10)
                warning = getattr(class_distribution, 'warning', False)
            rows = [[
                Paragraph("Class", table_header_style),
                Paragraph("Count", table_header_style),
                Paragraph("Proportion", table_header_style),
            ]]
            for cls, cnt in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
                prop = proportions.get(cls, 0.0)
                rows.append([
                    Paragraph(str(cls), table_cell_style),
                    Paragraph(str(int(cnt)), table_cell_style),
                    Paragraph(f"{prop:.2%}", table_cell_style),
                ])
            cd_table = Table(rows, colWidths=[available_width * 0.5, available_width * 0.2, available_width * 0.3])
            cd_table.setStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#B0B7BF')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor('#ECF0F1')]),
            ])
            story.append(cd_table)
            # Optional class distribution bar plot (counts)
            try:
                if counts:
                    import plotly.express as _px_cd
                    _df_cd = pd.DataFrame({
                        'Class': list(counts.keys()),
                        'Count': [int(v) for v in counts.values()],
                    }).sort_values('Count', ascending=False)
                    _fig_cd = _px_cd.bar(_df_cd, x='Class', y='Count', title='Class Distribution (Counts)', template='plotly_white')
                    # Render at high resolution to avoid pixelation
                    _img_name = f"{os.path.splitext(os.path.basename(file_identifier or 'report'))[0]}_class_dist.png"
                    _img_dir = os.path.dirname(output_path_or_buffer) if isinstance(output_path_or_buffer, str) else '.'
                    _img_path = os.path.join(_img_dir, _img_name)
                    _fig_cd.write_image(_img_path, format='png', width=1600, height=800, scale=1)
                    story.append(Spacer(1, 6))
                    # Preserve aspect ratio with hard caps to avoid layout breakage
                    try:
                        _pil_img = PILImage.open(_img_path)
                        _w, _h = _pil_img.size
                        _max_w = available_width * 0.85
                        _max_h = available_height * 0.35
                        _scale = min(_max_w / float(_w), _max_h / float(_h))
                        _disp_w = max(1.0, _w * _scale)
                        _disp_h = max(1.0, _h * _scale)
                        story.append(Image(_img_path, width=_disp_w, height=_disp_h))
                    except Exception:
                        story.append(Image(_img_path, width=available_width * 0.8, height=available_height * 0.3))
            except Exception:
                # Keep report generation resilient if image export fails
                pass
            if warning:
                story.append(Spacer(1, 6))
                story.append(Paragraph(
                    f"Severe imbalance flagged (minority < {warn_threshold:.0%}).",
                    styles['Normal']
                ))
            story.append(Spacer(1, SPACING_L))

        # Imputation Stability & Bias (optional)
        if isinstance(bias_diagnostics, pd.DataFrame) and not bias_diagnostics.empty:
            story.append(Paragraph("Imputation Stability & Bias", section_header_style))
            # Stability summary (if provided)
            if isinstance(stability_diagnostics, pd.DataFrame) and not stability_diagnostics.empty:
                story.append(Spacer(1, 6))
                story.append(Paragraph("Stability (repeatability)", subsection_header_style))
                try:
                    # Show top variables by worst (highest) cv_error
                    df_stab = stability_diagnostics.copy()
                    df_stab = df_stab.sort_values(by=['cv_error','mean_error'], ascending=[False, True]).head(20)
                    # Round for display
                    for c in ('mean_error','sd_error','cv_error'):
                        if c in df_stab.columns:
                            df_stab[c] = pd.to_numeric(df_stab[c], errors='coerce').round(4)
                    block = build_dataframe_table(df_stab, title="Imputation Stability (top variables)", max_rows=50)
                    story.append(KeepTogether(block))
                except Exception:
                    pass
            # Show thresholds if provided
            if isinstance(bias_thresholds, dict) and bias_thresholds:
                thr_text = \
                    f"SMD≥{bias_thresholds.get('smd_threshold', 0.10)} | " \
                    f"Var-ratio∉[{bias_thresholds.get('var_ratio_low', 0.5)},{bias_thresholds.get('var_ratio_high', 2.0)}] | " \
                    f"KS p<{bias_thresholds.get('ks_alpha', 0.05)}"
                story.append(Paragraph(f"<b>Warning rules:</b> {thr_text}", styles['Normal']))
                story.append(Spacer(1, 6))
            # Compute rule triggers per variable for explainability
            _smd_thr = float(bias_thresholds.get('smd_threshold', 0.10)) if isinstance(bias_thresholds, dict) else 0.10
            _var_lo = float(bias_thresholds.get('var_ratio_low', 0.5)) if isinstance(bias_thresholds, dict) else 0.5
            _var_hi = float(bias_thresholds.get('var_ratio_high', 2.0)) if isinstance(bias_thresholds, dict) else 2.0
            _ks_alpha = float(bias_thresholds.get('ks_alpha', 0.05)) if isinstance(bias_thresholds, dict) else 0.05

            def _trigger_text(row) -> str:
                reasons = []
                try:
                    val = pd.to_numeric(row.get('smd'), errors='coerce')
                    if pd.notnull(val) and abs(float(val)) >= _smd_thr:
                        reasons.append(f"SMD≥{_smd_thr}")
                except Exception:
                    pass
                try:
                    vr = pd.to_numeric(row.get('var_ratio'), errors='coerce')
                    if pd.notnull(vr) and (float(vr) < _var_lo or float(vr) > _var_hi):
                        reasons.append(f"Var-ratio∉[{_var_lo},{_var_hi}]")
                except Exception:
                    pass
                try:
                    pval = pd.to_numeric(row.get('ks_p'), errors='coerce')
                    if pd.notnull(pval) and float(pval) < _ks_alpha:
                        reasons.append(f"KS p<{_ks_alpha}")
                except Exception:
                    pass
                return "; ".join(reasons)

            cols_desired = [
                ("column", "Variable"), ("n_obs", "n_obs"), ("n_imp", "n_imp"),
                ("smd", "SMD"), ("var_ratio", "Var-ratio"), ("ks_p", "KS p"),
                ("triggers", "Triggers"), ("warn", "Warn")
            ]
            df_bias = bias_diagnostics.copy()
            present = [src for src, _ in cols_desired if src in df_bias.columns]
            # Derive triggers before column selection to ensure availability
            try:
                df_bias['triggers'] = df_bias.apply(_trigger_text, axis=1)
            except Exception:
                df_bias['triggers'] = ""
            df_bias = df_bias[[c for c in [src for src, _ in cols_desired] if c in df_bias.columns]]
            # Rename headers for readability
            rename_map = {src: label for src, label in cols_desired if src in df_bias.columns}
            df_bias = df_bias.rename(columns=rename_map)
            # Round numeric columns
            for c in df_bias.columns:
                if c in ("SMD", "Var-ratio", "KS p"):
                    df_bias[c] = pd.to_numeric(df_bias[c], errors='coerce').round(3)
            block = []
            block.append(Paragraph(f"<b>Variables evaluated:</b> {len(df_bias)}", styles['Normal']))
            block.extend(build_dataframe_table(df_bias, title="", max_rows=50))
            story.append(KeepTogether(block))

            # Traffic-light status bar for top variables (red=warn, green=ok)
            try:
                status_rows = []
                header = [Paragraph("Variable", table_header_style), Paragraph("Status", table_header_style), Paragraph("Triggers", table_header_style)]
                status_rows.append(header)
                show_df = bias_diagnostics.sort_values(by=['warn', 'smd'], ascending=[False, False]).head(20)
                for _, r in show_df.iterrows():
                    var = str(r.get('column'))
                    warn_flag = bool(r.get('warn', False))
                    status_cell = Paragraph("WARN" if warn_flag else "OK", table_cell_style)
                    trig = _trigger_text(r)
                    status_rows.append([Paragraph(var, table_cell_style), status_cell, Paragraph(trig or "—", table_cell_style)])
                status_tbl = Table(status_rows, colWidths=[available_width * 0.5, available_width * 0.15, available_width * 0.35])
                status_tbl.setStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#B0B7BF')),
                ])
                # Color status cells
                for i in range(1, len(status_rows)):
                    warn_flag = bool(show_df.iloc[i-1].get('warn', False))
                    color = colors.red if warn_flag else colors.green
                    status_tbl.setStyle([('BACKGROUND', (1, i), (1, i), color), ('TEXTCOLOR', (1, i), (1, i), colors.white)])
                story.append(Spacer(1, 6))
                story.append(status_tbl)
            except Exception:
                logging.exception("Failed to render traffic-light status table for bias diagnostics.")

            story.append(Spacer(1, SPACING_L))

        # Missing Data Summary (table)
        story.append(Paragraph("Missing Data Summary", section_header_style))
        if isinstance(missing_data, pd.Series) or isinstance(missing_data, dict):
            md_items = sorted(list(missing_data.items()), key=lambda kv: (-int(kv[1]), str(kv[0])))
            md_rows = [[Paragraph("Column", table_header_style), Paragraph("Missing Count", table_header_style)]]
            for col, cnt in md_items:
                md_rows.append([Paragraph(str(col), table_cell_style), Paragraph(str(int(cnt)), table_cell_style)])
            md_table = Table(md_rows, colWidths=[available_width * 0.6, available_width * 0.4])
            md_table.setStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#B0B7BF')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor('#ECF0F1')]),
            ])
            story.append(md_table)
        else:
            story.append(Paragraph("No missing data summary available.", styles['Normal']))
        story.append(Spacer(1, SPACING_L))

        # Records Flagged for Missing Data
        story.append(Paragraph(f"<b>Records Flagged for Missing Data:</b> {flagged_records_count}", styles['Normal']))
        story.append(Spacer(1, 12))

        # Ontology Mapping Success Rates (table)
        story.append(Paragraph("Ontology Mapping Summary", section_header_style))
        if mapping_success_rates:
            map_rows = [[
                Paragraph("Ontology", table_header_style),
                Paragraph("Total Terms", table_header_style),
                Paragraph("Mapped", table_header_style),
                Paragraph("Success Rate", table_header_style),
            ]]
            for ontology_id, stats in mapping_success_rates.items():
                map_rows.append([
                    Paragraph(ontology_id, table_cell_style),
                    Paragraph(str(int(stats.get('total_terms', 0))), table_cell_style),
                    Paragraph(str(int(stats.get('mapped_terms', 0))), table_cell_style),
                    Paragraph(f"{float(stats.get('success_rate', 0)):.2f}%", table_cell_style),
                ])
            map_table = Table(
                map_rows,
                colWidths=[available_width * 0.25, available_width * 0.25, available_width * 0.2, available_width * 0.3]
            )
            map_table.setStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#B0B7BF')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor('#ECF0F1')]),
            ])
            story.append(map_table)
        else:
            story.append(Paragraph("No mapping statistics available.", styles['Normal']))
        story.append(Spacer(1, SPACING_L))
        story.append(hr())
        story.append(Spacer(1, SPACING_L))

        # Visualizations (grid, two per row)
        # Start visualizations on a new page only when there are images to show
        if visualization_images:
            story.append(PageBreak())
            story.append(Paragraph("Visualizations", section_header_style))
        if visualization_images:
            col_w = (available_width - 12) / 2  # small gutter
            rows = []
            row = []
            for idx, image_path in enumerate(visualization_images):
                if os.path.exists(image_path):
                    # Preserve aspect ratio: compute height based on image dimensions
                    try:
                        pil_img = PILImage.open(image_path)
                        iw, ih = pil_img.size
                        # Cap height to avoid overflows
                        disp_h = min(col_w * (ih / iw), available_height * 0.38)
                        img = Image(image_path, width=col_w, height=disp_h)
                    except Exception:
                        img = Image(image_path, width=col_w, height=col_w * 0.6)
                    row.append(img)
                else:
                    row.append(Paragraph(f"Image not found: {image_path}", styles['Normal']))
                if len(row) == 2:
                    rows.append(row)
                    row = []
            if row:
                # Pad last row
                while len(row) < 2:
                    row.append(Spacer(1, 1))
                rows.append(row)
            img_table = Table(rows, colWidths=[col_w, col_w], hAlign='CENTER', spaceBefore=6, spaceAfter=6)
            img_table.setStyle([
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (-1, -1), 6),
                ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ])
            story.append(img_table)
        else:
            story.append(Paragraph("No visualizations generated.", styles['Normal']))

        doc = SimpleDocTemplate(
            output_path_or_buffer,
            pagesize=page_size,
            leftMargin=left_margin,
            rightMargin=right_margin,
            topMargin=top_margin,
            bottomMargin=bottom_margin,
        )
        # Add simple page numbers
        def _add_page_number(canvas_obj, doc_obj):
            page_num_text = f"Page {canvas_obj.getPageNumber()}"
            canvas_obj.setFont('Helvetica', 8)
            canvas_obj.setFillColor(colors.HexColor('#7F8C8D'))
            canvas_obj.drawRightString(page_size[0] - right_margin, bottom_margin - 10, page_num_text)

        doc.build(story, onFirstPage=_add_page_number, onLaterPages=_add_page_number)

    elif report_format == 'md':
        md_lines = [
            "# PhenoQC Quality Control Report\n",
            "## Imputation Strategy Used",
            f"{impute_strategy.capitalize() if impute_strategy else '(No Imputation Strategy)'}\n",
            "",
            "## Data Quality Scores",
        ]
        for score_name, score_value in quality_scores.items():
            md_lines.append(f"- **{score_name}**: {score_value:.2f}%")
        md_lines.append("")
        # Optional class distribution
        if class_distribution:
            md_lines.append("## Class Distribution")
            counts = getattr(class_distribution, 'counts', {})
            proportions = getattr(class_distribution, 'proportions', {})
            warn_threshold = getattr(class_distribution, 'warn_threshold', 0.10)
            for cls, cnt in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
                md_lines.append(f"- {cls}: {cnt} ({proportions.get(cls, 0.0):.2%})")
            if getattr(class_distribution, 'warning', False):
                md_lines.append(f"\n> Severe imbalance flagged (minority < {warn_threshold:.0%}).\n")
            # Save and embed a class distribution bar chart next to the MD file
            try:
                if isinstance(output_path_or_buffer, str) and counts:
                    import plotly.express as _px_cd_md
                    _df_cd_md = pd.DataFrame({
                        'Class': list(counts.keys()),
                        'Count': [int(v) for v in counts.values()],
                    }).sort_values('Count', ascending=False)
                    _fig_cd_md = _px_cd_md.bar(_df_cd_md, x='Class', y='Count', title='Class Distribution (Counts)', template='plotly_white')
                    _out_dir_md = os.path.dirname(output_path_or_buffer)
                    _base_md = os.path.splitext(os.path.basename(file_identifier or 'report'))[0]
                    _img_name_md = f"{_base_md}_class_dist.png"
                    _img_path_md = os.path.join(_out_dir_md, _img_name_md)
                    _fig_cd_md.write_image(_img_path_md, format='png', width=1800, height=900, scale=1)
                    md_lines.append("")
                    md_lines.append(f"![Class Distribution]({_img_name_md})")
                    md_lines.append("")
            except Exception:
                # Keep MD generation resilient if image export fails
                pass
            md_lines.append("")

        # Imputation Stability & Bias (optional)
        if isinstance(bias_diagnostics, pd.DataFrame) and not bias_diagnostics.empty:
            md_lines.append("## Imputation Stability & Bias")
            # Stability snippet (if provided)
            if isinstance(stability_diagnostics, pd.DataFrame) and not stability_diagnostics.empty:
                try:
                    _df_st = stability_diagnostics.copy()
                    _df_st = _df_st.sort_values(by=['cv_error','mean_error'], ascending=[False, True]).head(20)
                    # Select columns if present
                    cols = [c for c in ['column','metric','repeats','mean_error','sd_error','cv_error'] if c in _df_st.columns]
                    _df_st = _df_st[cols]
                    md_lines.append(_df_st.to_markdown(index=False))
                except Exception:
                    try:
                        md_lines.append(stability_diagnostics.to_csv(index=False))
                    except Exception:
                        pass
            if isinstance(bias_thresholds, dict) and bias_thresholds:
                md_lines.append(
                    f"- Thresholds: SMD≥{bias_thresholds.get('smd_threshold', 0.10)}, "
                    f"Var-ratio outside [{bias_thresholds.get('var_ratio_low', 0.5)},{bias_thresholds.get('var_ratio_high', 2.0)}], "
                    f"KS p<{bias_thresholds.get('ks_alpha', 0.05)}"
                )
            # Add a Triggers column explaining why WARN
            _smd_thr = float(bias_thresholds.get('smd_threshold', 0.10)) if isinstance(bias_thresholds, dict) else 0.10
            _var_lo = float(bias_thresholds.get('var_ratio_low', 0.5)) if isinstance(bias_thresholds, dict) else 0.5
            _var_hi = float(bias_thresholds.get('var_ratio_high', 2.0)) if isinstance(bias_thresholds, dict) else 2.0
            _ks_alpha = float(bias_thresholds.get('ks_alpha', 0.05)) if isinstance(bias_thresholds, dict) else 0.05
            try:
                _df_md = bias_diagnostics.copy()
                def _trig_md(row):
                    rs = []
                    try:
                        v = float(row.get('smd'))
                        if abs(v) >= _smd_thr:
                            rs.append(f"SMD≥{_smd_thr}")
                    except Exception:
                        pass
                    try:
                        vr = float(row.get('var_ratio'))
                        if vr < _var_lo or vr > _var_hi:
                            rs.append(f"Var-ratio∉[{_var_lo},{_var_hi}]")
                    except Exception:
                        pass
                    try:
                        p = float(row.get('ks_p'))
                        if p < _ks_alpha:
                            rs.append(f"KS p<{_ks_alpha}")
                    except Exception:
                        pass
                    return "; ".join(rs)
                _df_md['triggers'] = _df_md.apply(_trig_md, axis=1)
                # Reorder if possible
                cols = [c for c in ['column','n_obs','n_imp','smd','var_ratio','ks_p','triggers','warn'] if c in _df_md.columns]
                _df_md = _df_md[cols]
                md_lines.append(_df_md.to_markdown(index=False))
            except Exception:
                md_lines.append(bias_diagnostics.to_csv(index=False))
            md_lines.append("")

        md_lines.append("## Schema Validation Results")
        for key, value in validation_results.items():
            if isinstance(value, pd.DataFrame):
                if not value.empty:
                    md_lines.append(f"- **{key}**: {len(value)} issues found.")
                else:
                    md_lines.append(f"- **{key}**: No issues found.")
            else:
                md_lines.append(f"- **{key}**: {value}")
        md_lines.append("")

        md_lines.append("## Additional Quality Dimensions")
        for metric in ["Accuracy Issues", "Redundancy Issues", "Traceability Issues", "Timeliness Issues"]:
            if metric in validation_results:
                df_metric = validation_results[metric]
                if isinstance(df_metric, pd.DataFrame) and not df_metric.empty:
                    md_lines.append(f"- **{metric}**: {len(df_metric)} issues found.")
                    try:
                        md_lines.append(df_metric.to_markdown(index=False))
                    except Exception:
                        md_lines.append(df_metric.to_csv(index=False))
                else:
                    md_lines.append(f"- **{metric}**: No issues found.")
        md_lines.append("")
        md_lines.append("## Missing Data Summary")
        for column, count in missing_data.items():
            md_lines.append(f"- **{column}**: {count} missing values")
        md_lines.append("")
        md_lines.append(f"**Records Flagged for Missing Data**: {flagged_records_count}\n")
        md_lines.append("## Ontology Mapping Success Rates")
        for ontology_id, stats in mapping_success_rates.items():
            md_lines.append(f"### {ontology_id}")
            md_lines.append(f"- **Total Terms**: {stats['total_terms']}")
            md_lines.append(f"- **Mapped Terms**: {stats['mapped_terms']}")
            md_lines.append(f"- **Success Rate**: {stats['success_rate']:.2f}%")
            md_lines.append("")
        md_lines.append("## Visualizations")
        for image_path in visualization_images:
            image_filename = os.path.basename(image_path)
            md_lines.append(f"![{image_filename}]({image_filename})")
            md_lines.append("")

        if isinstance(output_path_or_buffer, str):
            with open(output_path_or_buffer, 'w') as f:
                f.write('\n'.join(md_lines))
        else:
            output_path_or_buffer.write('\n'.join(md_lines).encode('utf-8'))
    else:
        raise ValueError("Unsupported report format. Use 'pdf' or 'md'.")


def create_visual_summary(df, phenotype_columns=None, output_image_path=None):
    """
    Creates visual summaries with extra steps to keep axis labels fully visible:
      1) Missingness Heatmap (white/blue)
      2) Bar plot of % missing per column
      3) Numeric histograms ignoring ID columns
      4) Optional bar/pie charts for phenotype columns
    """
    # Check for proper DataFrame input
    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            f"create_visual_summary() expects a pandas DataFrame, but got {type(df)}."
        )

    figs = []

    # 1) Missingness visuals
    if not df.empty:
        # (a) Heatmap
        figs.append(create_missingness_heatmap(df))
        # (b) Missing distribution
        figs.append(create_missingness_distribution(df))
        # (c) Numeric histograms
        possible_ids = [c for c in df.columns if "id" in c.lower()]
        figs.extend(create_numeric_histograms(df, unique_id_cols=possible_ids))

    # 2) Phenotype-based plots
    if phenotype_columns:
        for column, ontologies in phenotype_columns.items():
            if column not in df.columns:
                continue
            non_null_values = df[column].dropna()
            if len(non_null_values) == 0:
                continue

            phenotype_counts = non_null_values.value_counts().head(20)
            fig_bar = px.bar(
                phenotype_counts,
                labels={'index': 'Phenotype Term', 'value': 'Count'},
                title=f"Top 20 Most Common Terms in {column}",
                template='plotly_white'
            )
            fig_bar.update_layout(
                plot_bgcolor="#FFFFFF",
                paper_bgcolor="#FFFFFF",
                font={'color': "#2C3E50", 'size': 12},
                title={
                    'text': f"Top 20 Most Common Terms in {column}",
                    'y': 0.97, 'x': 0.45,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': {'size': 16}
                },
                showlegend=False,
                width=1200,
                height=700,
                margin=dict(t=120, b=200, l=140, r=120),
                bargap=0.25
            )
            fig_bar.update_xaxes(
                tickangle=60,
                automargin=True,
                tickfont={'size': 10},
                ticktext=[
                    f"{text[:40]}..." if len(text) > 40 else text
                    for text in phenotype_counts.index
                ],
                tickvals=list(range(len(phenotype_counts))),
                showticklabels=True,
                tickmode='array'
            )
            figs.append(fig_bar)

            for onto_id in ontologies:
                mapped_col = f"{onto_id}_ID"
                if mapped_col not in df.columns:
                    continue
                valid_terms = ~df[column].isin([
                    'NotARealTerm','ZZZZ:9999999','PhenotypeJunk','InvalidTerm42'
                ])
                total = df[column].notna() & valid_terms
                total_count = total.sum()
                mapped = df[mapped_col].notna() & total
                mapped_count = mapped.sum()
                unmapped_count = total_count - mapped_count

                fig_pie = go.Figure(data=[go.Pie(
                    labels=['Mapped', 'Unmapped'],
                    values=[mapped_count, unmapped_count],
                    hole=0.4,
                    marker=dict(colors=['#4C72B0', '#DD8452']),
                    textinfo='label+percent',
                    textposition='outside',
                    textfont={'size': 14},
                    hovertemplate="<b>%{label}</b><br>Count: %{value}"
                                  "<br>Percentage: %{percent}<extra></extra>"
                )])
                fig_pie.update_layout(
                    title={
                        'text': f"Mapping Results: {column} → {onto_id}",
                        'y': 0.95,
                        'x': 0.5,
                        'xanchor': 'center',
                        'yanchor': 'top',
                        'font': {'size': 16}
                    },
                    annotations=[{
                        'text': (
                            f"Total Valid Terms: {total_count}<br>"
                            f"Mapped: {mapped_count} "
                            f"({(mapped_count / total_count * 100 if total_count else 0):.1f}%)<br>"
                            f"Unmapped: {unmapped_count} "
                            f"({(unmapped_count / total_count * 100 if total_count else 0):.1f}%)"
                        ),
                        'x': 0.5,
                        'y': -0.2,
                        'showarrow': False,
                        'font': {'size': 12}
                    }],
                    showlegend=True,
                    legend={
                        'orientation': 'h',
                        'yanchor': 'bottom',
                        'y': -0.3,
                        'xanchor': 'center',
                        'x': 0.5
                    },
                    width=900,
                    height=700,
                    plot_bgcolor="#FFFFFF",
                    paper_bgcolor="#FFFFFF",
                    font={'color': "#2C3E50"},
                    margin=dict(t=120, b=180, l=100, r=100)
                )
                figs.append(fig_pie)

    return figs

def create_missingness_distribution(df):
    """
    Returns a bar chart showing percent missingness per column.
    """
    missing_count = df.isna().sum()
    missing_percent = (missing_count / len(df)) * 100
    data = pd.DataFrame({
        "column": missing_count.index,
        "percent_missing": missing_percent
    }).sort_values("percent_missing", ascending=True)

    fig = px.bar(
        data,
        x="percent_missing",
        y="column",
        orientation="h",
        title="Percentage of Missing Data by Column",
        template="plotly_white",
        color_discrete_sequence=["#d62728"]
    )
    fig.update_layout(
        height=500,
        width=800,
        margin=dict(l=120, r=80, t=60, b=60),
        font=dict(size=12)
    )
    fig.update_xaxes(title_text="Percent Missing", automargin=True)
    fig.update_yaxes(title_text="Columns", automargin=True)
    return fig

def create_missingness_heatmap(df):
    """
    Generates a missingness heatmap with exactly two colors:
    White for present (0) and a pleasing blue (#3B82F6) for missing (1).
    """
    missing_matrix = df.isna().astype(int)
    col_order = missing_matrix.sum().sort_values(ascending=False).index
    missing_matrix = missing_matrix[col_order]

    two_color_scale = [(0.0, "white"), (1.0, "#3B82F6")]

    # Build the base heatmap
    fig = px.imshow(
        missing_matrix,
        zmin=0,
        zmax=1,
        color_continuous_scale=two_color_scale,
        labels={"color": "Missing"},
        aspect="auto",
        title="Missingness Heatmap"
    )
    # Bump the figure size
    fig.update_layout(
        height=800,
        width=1200,
        # Extra space for big labels & a lower-located chart title
        margin=dict(l=130, r=130, t=180, b=200),
        font=dict(size=12),
        xaxis=dict(side="top"),
    )
    # Move the chart title downward so it's clearly separate from x-labels
    fig.update_layout(
        title=dict(
            text="Missingness Heatmap",
            x=0.5,
            y=0.90,      # Move the title down a bit more
            xanchor="center",
            yanchor="bottom"
        )
    )
    # Increase standoff for the x-axis label
    fig.update_xaxes(
        title=dict(text="Columns", standoff=70),
        tickangle=80,  # or 90 to make them vertical
        automargin=True
    )
    # Extra standoff for y-axis label if needed
    fig.update_yaxes(
        title=dict(text="Rows", standoff=20),
        automargin=True
    )
    return fig

def create_numeric_histograms(df, unique_id_cols=None, max_cols=5):
    """
    Creates histogram figures for numeric columns, ignoring any columns
    that appear in `unique_id_cols` (if provided).
    """
    if unique_id_cols is None:
        unique_id_cols = []
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    numeric_cols = [col for col in numeric_cols if col not in unique_id_cols]
    numeric_cols = numeric_cols[:max_cols]

    figs = []
    for col in numeric_cols:
        fig = px.histogram(
            df,
            x=col,
            nbins=30,
            title=f"Distribution of {col}",
            template="plotly_white",
            color_discrete_sequence=["#1f77b4"]
        )
        fig.update_layout(
            height=400,
            width=600,
            margin=dict(l=60, r=60, t=60, b=60),
            font=dict(size=12),
        )
        figs.append(fig)
    return figs
