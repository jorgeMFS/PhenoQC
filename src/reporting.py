import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import os
import io

def generate_qc_report(validation_results, missing_data, flagged_records_count, output_path_or_buffer):
    """
    Generates a PDF quality control report.

    Args:
        validation_results (dict): Results from schema validation.
        missing_data (pd.Series): Series with counts of missing data per column.
        flagged_records_count (int): Number of records flagged for missing data.
        output_path_or_buffer (str or BytesIO): Path or buffer to save the PDF report.
    """
    if isinstance(output_path_or_buffer, str):
        c = canvas.Canvas(output_path_or_buffer, pagesize=letter)
    else:
        c = canvas.Canvas(output_path_or_buffer, pagesize=letter)

    width, height = letter
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 50, "PhenoQC Quality Control Report")

    c.setFont("Helvetica", 12)
    c.drawString(50, height - 80, "Schema Validation Results:")
    y = height - 100
    for key, value in validation_results.items():
        c.drawString(60, y, f"{key}: {value}")
        y -= 20

    y -= 10
    c.drawString(50, y, "Missing Data Summary:")
    y -= 20
    for column, count in missing_data.items():
        c.drawString(60, y, f"{column}: {count} missing values")
        y -= 20

    y -= 10
    c.drawString(50, y, f"Records Flagged for Missing Data: {flagged_records_count}")
    y -= 20

    c.save()

def create_visual_summary(df, output_image_path="visual_summary.html"):
    """
    Creates interactive visual summaries of the data.

    Args:
        df (pd.DataFrame): The processed data frame.
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
    if 'Phenotype' in df.columns:
        phenotype_counts = df['Phenotype'].value_counts()
        if not phenotype_counts.empty:
            fig2 = px.bar(
                phenotype_counts,
                labels={'index': 'Phenotype', 'value': 'Count'},
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
