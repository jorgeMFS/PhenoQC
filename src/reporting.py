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
        story.append(Paragraph(f"{impute_strategy.capitalize()}", styles['Normal']))
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

def create_visual_summary(df, phenotype_column='Phenotype', output_image_path="reports/visual_summary.html"):
    """
    Creates interactive visual summaries of the data.

    Args:
        df (pd.DataFrame): The processed data frame.
        phenotype_column (str): The name of the column containing phenotypic terms.
        output_image_path (str): Path to save the visualization HTML file.

    Returns:
        list: List of Plotly figure objects.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input `df` must be a pandas DataFrame.")

    figs = []

    # Missing Data Heatmap
    missing_data = df.isnull()
    if missing_data.any().any():
        fig1 = px.imshow(
            missing_data,
            labels=dict(x="Columns", y="Records", color="Missing"),
            title="Missing Data Heatmap"
        )
        figs.append(fig1)
    else:
        # Optionally, add a message or a dummy plot indicating no missing data
        fig1 = go.Figure()
        fig1.add_annotation(
            x=0.5, y=0.5,
            text="No Missing Data",
            showarrow=False,
            font=dict(size=20)
        )
        fig1.update_layout(
            title="Missing Data Heatmap",
            xaxis={'visible': False},
            yaxis={'visible': False}
        )
        figs.append(fig1)

    # Distribution of Phenotypes
    if phenotype_column in df.columns:
        phenotype_counts = df[phenotype_column].value_counts()
        if not phenotype_counts.empty:
            fig2 = px.bar(
                phenotype_counts,
                labels={'index': phenotype_column, 'value': 'Count'},
                title='Distribution of Phenotypic Traits'
            )
            figs.append(fig2)

    # Pie Charts for Each Ontology
    ontology_columns = [col for col in df.columns if col.endswith('_ID')]
    for ontology_column in ontology_columns:
        mapped = df[ontology_column].notnull().sum()
        unmapped = df[ontology_column].isnull().sum()
        fig = go.Figure(data=[go.Pie(labels=['Mapped', 'Unmapped'], values=[mapped, unmapped])])
        fig.update_layout(title=f'Mapped vs Unmapped Phenotypic Terms ({ontology_column})')
        figs.append(fig)

    if output_image_path:
        # Create a single HTML file with all figures
        with open(output_image_path, 'w') as f:
            for idx, fig in enumerate(figs):
                f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))
    else:
        # Return the list of figures
        return figs
