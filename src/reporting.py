import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
import os

def generate_qc_report(
    validation_results,
    missing_data,
    flagged_records_count,
    mapping_success_rates,
    visualization_images,
    impute_strategy,
    quality_scores,
    output_path_or_buffer,
    report_format='pdf'
):
    """
    Generates a quality control report.

    Args:
        validation_results (dict): Results from schema validation.
        missing_data (pd.Series): Series with counts of missing data per column.
        flagged_records_count (int): Number of records flagged for missing data.
        mapping_success_rates (dict): Ontology mapping success rates.
        visualization_images (list): List of paths to visualization images.
        impute_strategy (str): Imputation strategy used.
        quality_scores (dict): Dictionary of data quality scores.
        output_path_or_buffer (str or BytesIO): Path or buffer to save the report.
        report_format (str): Format of the report ('pdf' or 'md').
    """
    if report_format == 'pdf':
        # Generate PDF report using ReportLab Platypus
        styles = getSampleStyleSheet()
        story = []

        # Title
        story.append(Paragraph("PhenoQC Quality Control Report", styles['Title']))
        story.append(Spacer(1, 12))

        # Imputation Strategy
        story.append(Paragraph("Imputation Strategy Used:", styles['Heading2']))
        if impute_strategy is None:
            strategy_display = "(No Imputation Strategy)"
        else:
            strategy_display = impute_strategy.capitalize()
        story.append(Paragraph(strategy_display, styles['Normal']))


        story.append(Spacer(1, 12))

        # Data Quality Scores
        story.append(Paragraph("Data Quality Scores:", styles['Heading2']))
        for score_name, score_value in quality_scores.items():
            story.append(Paragraph(f"{score_name}: {score_value:.2f}%", styles['Normal']))
        story.append(Spacer(1, 12))

        # Schema Validation Results
        story.append(Paragraph("Schema Validation Results:", styles['Heading2']))
        for key, value in validation_results.items():
            if isinstance(value, pd.DataFrame) and not value.empty:
                story.append(Paragraph(f"{key}: {len(value)} issues found.", styles['Normal']))
            else:
                story.append(Paragraph(f"{key}: {value}", styles['Normal']))
        story.append(Spacer(1, 12))

        # Missing Data Summary
        story.append(Paragraph("Missing Data Summary:", styles['Heading2']))
        for column, count in missing_data.items():
            story.append(Paragraph(f"{column}: {count} missing values", styles['Normal']))
        story.append(Spacer(1, 12))

        # Records Flagged for Missing Data
        story.append(Paragraph(f"Records Flagged for Missing Data: {flagged_records_count}", styles['Normal']))
        story.append(Spacer(1, 12))

        # Ontology Mapping Success Rates
        story.append(Paragraph("Ontology Mapping Success Rates:", styles['Heading2']))
        for ontology_id, stats in mapping_success_rates.items():
            story.append(Paragraph(f"{ontology_id}:", styles['Heading3']))
            story.append(Paragraph(f"Total Terms: {stats['total_terms']}", styles['Normal']))
            story.append(Paragraph(f"Mapped Terms: {stats['mapped_terms']}", styles['Normal']))
            story.append(Paragraph(f"Success Rate: {stats['success_rate']:.2f}%", styles['Normal']))
            story.append(Spacer(1, 12))

        # Visualizations
        story.append(Paragraph("Visualizations:", styles['Heading2']))
        for image_path in visualization_images:
            # Ensure the image exists before adding
            if os.path.exists(image_path):
                img = Image(image_path, width=6 * inch, height=4 * inch)
                story.append(img)
                story.append(Spacer(1, 12))
            else:
                story.append(Paragraph(f"Image not found: {image_path}", styles['Normal']))

        # Build the PDF
        if isinstance(output_path_or_buffer, str):
            doc = SimpleDocTemplate(output_path_or_buffer, pagesize=letter)
        else:
            doc = SimpleDocTemplate(output_path_or_buffer, pagesize=letter)
        doc.build(story)
    elif report_format == 'md':
        # Generate Markdown report
        md_lines = []
        md_lines.append("# PhenoQC Quality Control Report\n")

        # Imputation Strategy
        md_lines.append("## Imputation Strategy Used")
        md_lines.append(f"{impute_strategy.capitalize()}\n")
        md_lines.append("\n")

        # Data Quality Scores
        md_lines.append("## Data Quality Scores")
        for score_name, score_value in quality_scores.items():
            md_lines.append(f"- **{score_name}**: {score_value:.2f}%")
        md_lines.append("")

        # Schema Validation Results
        md_lines.append("## Schema Validation Results")
        for key, value in validation_results.items():
            if isinstance(value, pd.DataFrame) and not value.empty:
                md_lines.append(f"- **{key}**: {len(value)} issues found.")
            else:
                md_lines.append(f"- **{key}**: {value}")
        md_lines.append("")

        # Missing Data Summary
        md_lines.append("## Missing Data Summary")
        for column, count in missing_data.items():
            md_lines.append(f"- **{column}**: {count} missing values")
        md_lines.append("")

        # Records Flagged for Missing Data
        md_lines.append(f"**Records Flagged for Missing Data**: {flagged_records_count}\n")

        # Ontology Mapping Success Rates
        md_lines.append("## Ontology Mapping Success Rates")
        for ontology_id, stats in mapping_success_rates.items():
            md_lines.append(f"### {ontology_id}")
            md_lines.append(f"- **Total Terms**: {stats['total_terms']}")
            md_lines.append(f"- **Mapped Terms**: {stats['mapped_terms']}")
            md_lines.append(f"- **Success Rate**: {stats['success_rate']:.2f}%")
            md_lines.append("")

        # Visualizations
        md_lines.append("## Visualizations")
        for image_path in visualization_images:
            image_filename = os.path.basename(image_path)
            md_lines.append(f"![{image_filename}]({image_filename})")
            md_lines.append("")

        # Write the markdown content to the file or buffer
        if isinstance(output_path_or_buffer, str):
            with open(output_path_or_buffer, 'w') as f:
                f.write('\n'.join(md_lines))
        else:
            output_path_or_buffer.write('\n'.join(md_lines).encode('utf-8'))
    else:
        raise ValueError("Unsupported report format. Use 'pdf' or 'md'.")


def create_visual_summary(df, phenotype_columns=None, output_image_path=None):
    """Creates visual summaries of the data with improved readability and styling."""
    figs = []
    
    colors = {
        'mapped': '#4C72B0',
        'unmapped': '#DD8452',
        'background': '#FFFFFF',
        'text': '#2C3E50'
    }

    if phenotype_columns:
        for column, ontologies in phenotype_columns.items():
            if column in df.columns:
                # Bar chart with improved text handling
                non_null_values = df[column].dropna()
                if len(non_null_values) > 0:
                    phenotype_counts = non_null_values.value_counts().head(20)
                    
                    fig = px.bar(
                        phenotype_counts,
                        labels={'index': 'Phenotype Term', 'value': 'Count'},
                        title=f'Top 20 Most Common Terms in {column}',
                        template='plotly_white'
                    )
                    
                    # Improved bar chart layout
                    fig.update_layout(
                        plot_bgcolor=colors['background'],
                        paper_bgcolor=colors['background'],
                        font={'color': colors['text'], 'size': 12},
                        title={
                            'text': f'Top 20 Most Common Terms in {column}',
                            'y': 0.95,
                            'x': 0.5,
                            'xanchor': 'center',
                            'yanchor': 'top',
                            'font': {'size': 16}
                        },
                        showlegend=False,
                        width=1200,  # Increased width
                        height=700,  # Increased height
                        margin=dict(
                            t=120,   # Top margin
                            b=200,   # Increased bottom margin for labels
                            l=100,   # Left margin
                            r=100    # Right margin
                        ),
                        bargap=0.2   # Increased gap between bars
                    )
                    
                    # Improved x-axis label handling
                    fig.update_xaxes(
                        tickangle=45,
                        tickfont={'size': 10},
                        ticktext=[f"{text[:40]}..." if len(text) > 40 else text 
                                for text in phenotype_counts.index],
                        tickvals=list(range(len(phenotype_counts))),
                        showticklabels=True,
                        tickmode='array'
                    )
                    
                    figs.append(fig)

                # Pie chart with improved layout
                for onto_id in ontologies:
                    mapped_col = f"{onto_id}_ID"
                    if mapped_col in df.columns:
                        valid_terms = ~df[column].isin(['NotARealTerm', 'ZZZZ:9999999', 'PhenotypeJunk', 'InvalidTerm42'])
                        total = df[column].notna() & valid_terms
                        total_count = total.sum()
                        
                        mapped = df[mapped_col].notna() & total
                        mapped_count = mapped.sum()
                        unmapped_count = total_count - mapped_count
                        
                        mapped_pct = (mapped_count / total_count * 100) if total_count > 0 else 0
                        unmapped_pct = (unmapped_count / total_count * 100) if total_count > 0 else 0
                        
                        fig = go.Figure(data=[go.Pie(
                            labels=['Mapped', 'Unmapped'],
                            values=[mapped_count, unmapped_count],
                            hole=0.4,
                            marker=dict(colors=[colors['mapped'], colors['unmapped']]),
                            textinfo='label+percent',
                            textposition='outside',
                            textfont={'size': 14},
                            hovertemplate="<b>%{label}</b><br>" +
                                        "Count: %{value}<br>" +
                                        "Percentage: %{percent}<br>" +
                                        "<extra></extra>"
                        )])
                        
                        # Improved pie chart layout
                        fig.update_layout(
                            title={
                                'text': f'Mapping Results: {column} → {onto_id}',
                                'y': 0.95,
                                'x': 0.5,
                                'xanchor': 'center',
                                'yanchor': 'top',
                                'font': {'size': 16}
                            },
                            annotations=[{
                                'text': f'Total Valid Terms: {total_count}<br>' +
                                       f'Mapped: {mapped_count} ({mapped_pct:.1f}%)<br>' +
                                       f'Unmapped: {unmapped_count} ({unmapped_pct:.1f}%)',
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
                            width=900,    # Increased width
                            height=700,   # Increased height
                            plot_bgcolor=colors['background'],
                            paper_bgcolor=colors['background'],
                            font={'color': colors['text']},
                            margin=dict(
                                t=120,    # Top margin
                                b=150,    # Increased bottom margin for legend
                                l=100,    # Left margin
                                r=100     # Right margin
                            )
                        )
                        figs.append(fig)

    return figs