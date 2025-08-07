import os
import hashlib
import inspect
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

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

from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch

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
    file_identifier=None
):
    """
    Generates a quality control report (PDF or Markdown).
    No changes to other files are required.
    """
    if report_format == 'pdf':
        styles = getSampleStyleSheet()
        story = []

        # Title
        story.append(Paragraph("PhenoQC Quality Control Report", styles['Title']))
        story.append(Spacer(1, 12))

        if file_identifier:
            story.append(Paragraph(f"<b>Source file:</b> {file_identifier}", styles['Normal']))
            story.append(Spacer(1, 12))

        # Imputation Strategy
        story.append(Paragraph("Imputation Strategy Used:", styles['Heading2']))
        strategy_display = "(No Imputation Strategy)" if impute_strategy is None else impute_strategy.capitalize()
        story.append(Paragraph(strategy_display, styles['Normal']))
        story.append(Spacer(1, 12))

        # Data Quality Scores
        story.append(Paragraph("Data Quality Scores:", styles['Heading2']))
        for score_name, score_value in quality_scores.items():
            story.append(Paragraph(f"<b>{score_name}:</b> {score_value:.2f}%", styles['Normal']))
        story.append(Spacer(1, 12))

        # Schema Validation Results
        story.append(Paragraph("Schema Validation Results:", styles['Heading2']))
        for key, value in validation_results.items():
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
        story.append(Spacer(1, 12))

        # Additional Quality Dimensions
        story.append(Paragraph("Additional Quality Dimensions:", styles['Heading2']))
        for metric in ["Accuracy Issues", "Redundancy Issues", "Traceability Issues", "Timeliness Issues"]:
            if metric in validation_results:
                df_metric = validation_results[metric]
                if isinstance(df_metric, pd.DataFrame) and not df_metric.empty:
                    story.append(Paragraph(
                        f"<b>{metric}:</b> {len(df_metric)} issues found.",
                        styles['Normal']
                    ))
                    table_data = [df_metric.columns.tolist()] + df_metric.values.tolist()
                    story.append(Table(table_data))
                else:
                    story.append(Paragraph(
                        f"<b>{metric}:</b> No issues found.",
                        styles['Normal']
                    ))
        story.append(Spacer(1, 12))

        # Missing Data Summary
        story.append(Paragraph("Missing Data Summary:", styles['Heading2']))
        for column, count in missing_data.items():
            story.append(Paragraph(f"<b>{column}:</b> {count} missing values", styles['Normal']))
        story.append(Spacer(1, 12))

        # Records Flagged for Missing Data
        story.append(Paragraph(f"<b>Records Flagged for Missing Data:</b> {flagged_records_count}", styles['Normal']))
        story.append(Spacer(1, 12))

        # Ontology Mapping Success Rates
        story.append(Paragraph("Ontology Mapping Success Rates:", styles['Heading2']))
        for ontology_id, stats in mapping_success_rates.items():
            story.append(Paragraph(f"{ontology_id}:", styles['Heading3']))
            story.append(Paragraph(f"<b>Total Terms:</b> {stats['total_terms']}", styles['Normal']))
            story.append(Paragraph(f"<b>Mapped Terms:</b> {stats['mapped_terms']}", styles['Normal']))
            story.append(Paragraph(f"<b>Success Rate:</b> {stats['success_rate']:.2f}%", styles['Normal']))
            story.append(Spacer(1, 12))

        # Visualizations
        story.append(Paragraph("Visualizations:", styles['Heading2']))
        for image_path in visualization_images:
            if os.path.exists(image_path):
                # Increase figure size to ensure labels are visible
                img = Image(image_path, width=6.5 * inch, height=5 * inch)
                story.append(img)
                story.append(Spacer(1, 12))
            else:
                story.append(Paragraph(f"Image not found: {image_path}", styles['Normal']))

        if isinstance(output_path_or_buffer, str):
            doc = SimpleDocTemplate(output_path_or_buffer, pagesize=letter)
        else:
            doc = SimpleDocTemplate(output_path_or_buffer, pagesize=letter)
        doc.build(story)

    elif report_format == 'md':
        md_lines = []
        md_lines.append("# PhenoQC Quality Control Report\n")
        md_lines.append("## Imputation Strategy Used")
        md_lines.append(f"{impute_strategy.capitalize() if impute_strategy else '(No Imputation Strategy)'}\n")
        md_lines.append("\n")
        md_lines.append("## Data Quality Scores")
        for score_name, score_value in quality_scores.items():
            md_lines.append(f"- **{score_name}**: {score_value:.2f}%")
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
