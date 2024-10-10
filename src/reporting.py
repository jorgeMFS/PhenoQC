import plotly.express as px
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import os

def generate_qc_report(validation_results, missing_data, output_path):
    """
    Generates a PDF quality control report.
    
    Args:
        validation_results (dict): Results from schema validation.
        missing_data (pd.Series): Series with counts of missing data per column.
        output_path (str): Path to save the PDF report.
    """
    c = canvas.Canvas(output_path, pagesize=letter)
    width, height = letter
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 50, "PhenoQC Quality Control Report")
    
    c.setFont("Helvetica", 12)
    c.drawString(50, height - 80, "Schema Validation Results:")
    y = height - 100
    for key, value in validation_results.items():
        c.drawString(60, y, f"{key}: {value}")
        y -= 20
    
    c.drawString(50, y - 20, "Missing Data Summary:")
    y -= 40
    for column, count in missing_data.items():
        c.drawString(60, y, f"{column}: {count} missing values")
        y -= 20
    
    c.save()

def create_visual_summary(missing_data, output_image_path="missing_data.png"):
    """
    Creates a bar chart summary of missing data.
    
    Args:
        missing_data (pd.Series): Series with counts of missing data per column.
        output_image_path (str): Path to save the visualization image.
    """
    if missing_data.empty:
        print("No missing data to visualize.")
        return
    fig = px.bar(x=missing_data.index, y=missing_data.values, 
                 labels={'x': 'Column', 'y': 'Missing Values'}, 
                 title='Missing Data by Column')
    fig.write_image(output_image_path)