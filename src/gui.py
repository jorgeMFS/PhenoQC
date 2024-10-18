import streamlit as st
from batch_processing import process_file  # Access the process_file function directly
import os
import tempfile
import zipfile
from configuration import load_config
from logging_module import setup_logging, log_activity
from mapping import OntologyMapper
import pandas as pd
from reporting import create_visual_summary, generate_qc_report
import json
import io

def extract_zip(zip_path, extract_to):
    """
    Extracts a ZIP archive to the specified directory.

    Args:
        zip_path (str): Path to the ZIP file.
        extract_to (str): Directory to extract the contents.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        return True, None
    except zipfile.BadZipFile:
        return False, "The uploaded file is not a valid ZIP archive."

def collect_supported_files(directory, supported_extensions):
    """
    Recursively collects all supported files from the specified directory,
    excluding macOS metadata files and directories.

    Args:
        directory (str): Path to the directory to scan.
        supported_extensions (set): Set of supported file extensions.

    Returns:
        list: List of valid file paths.
    """
    collected_files = []
    for root, dirs, files in os.walk(directory):
        # Exclude __MACOSX directories
        dirs[:] = [d for d in dirs if d != '__MACOSX']
        
        for file in files:
            # Exclude files starting with '._'
            if file.startswith('._'):
                continue
            
            ext = os.path.splitext(file)[1].lower()
            if ext in supported_extensions:
                collected_files.append(os.path.join(root, file))
    return collected_files

def main():
    # Setup logging
    setup_logging()

    # Configure Streamlit page
    st.set_page_config(page_title="PhenoQC - Phenotypic Data QC Toolkit", layout="wide")
    st.title("PhenoQC - Phenotypic Data Quality Control Toolkit")
    
    # Sidebar Configuration
    st.sidebar.header("Configuration")
    schema_file = st.sidebar.file_uploader("Upload JSON Schema", type=["json"])
    config_file = st.sidebar.file_uploader("Upload Configuration (config.yaml)", type=["yaml", "yml"])
    custom_mapping_file = st.sidebar.file_uploader("Upload Custom Mapping (Optional)", type=["json"])
    impute_strategy = st.sidebar.selectbox(
        "Imputation Strategy",
        options=['mean', 'median', 'mode', 'knn', 'mice', 'svd', 'none'],  
        index=0,
        help="Select the strategy to impute missing data."
    )
    unique_identifiers = st.sidebar.text_input(
        "Unique Identifier Columns (comma-separated)",
        help="Enter column names separated by commas, e.g., SampleID,PatientID"
    )
    ontologies = st.sidebar.text_input(
        "Ontologies to Map (space-separated)",
        help="Enter ontology IDs separated by space, e.g., HPO DO MPO"
    )

    
    # Sidebar Data Ingestion Options
    st.sidebar.header("Data Ingestion")
    data_source_option = st.sidebar.radio("Select Data Source", options=['Upload Files', 'Upload Directory (ZIP)'])
    
    if data_source_option == 'Upload Files':
        uploaded_files = st.file_uploader(
            "Upload Phenotypic Data Files",
            type=["csv", "tsv", "json"],
            accept_multiple_files=True
        )
    else:
        uploaded_zip = st.file_uploader(
            "Upload Data Directory (ZIP Archive)",
            type=["zip"],
            help="Compress your data directory into a ZIP file before uploading."
        )
    
    enable_recursive = st.sidebar.checkbox("Enable Recursive Directory Scanning", value=True)
    
    if st.button("Run Quality Control"):
        # Validate required uploads
        if not schema_file or not config_file:
            st.error("Please upload both the JSON schema and configuration (config.yaml) files.")
            return
        
        if data_source_option == 'Upload Files' and not uploaded_files:
            st.error("Please upload at least one phenotypic data file.")
            return
        elif data_source_option == 'Upload Directory (ZIP)' and not uploaded_zip:
            st.error("Please upload a ZIP archive containing your data files.")
            return
        
        # Validate unique identifiers
        if not unique_identifiers.strip():
            st.error("Please specify unique identifier columns.")
            return
        
        # Prepare unique identifiers
        unique_identifiers_list = [uid.strip() for uid in unique_identifiers.split(',') if uid.strip()]
        if not unique_identifiers_list:
            st.error("Please provide at least one unique identifier column.")
            return
            
        # Prepare ontologies list
        ontologies_list = [ont.strip() for ont in ontologies.split() if ont.strip()]
        if not ontologies_list:
            ontologies_list = None  # Defaults in process_file

        # Save uploaded files to a temporary directory
        with tempfile.TemporaryDirectory() as tmpdirname:
            input_paths = []
            
            # Handle individual file uploads
            if data_source_option == 'Upload Files':
                for uploaded_file in uploaded_files:
                    file_path = os.path.join(tmpdirname, uploaded_file.name)
                    with open(file_path, 'wb') as f:
                        f.write(uploaded_file.getbuffer())
                    input_paths.append(file_path)
            else:
                # Handle ZIP archive upload
                zip_path = os.path.join(tmpdirname, "data_archive.zip")
                with open(zip_path, 'wb') as f:
                    f.write(uploaded_zip.getbuffer())
                
                # Extract ZIP archive
                extract_dir = os.path.join(tmpdirname, "extracted")
                os.makedirs(extract_dir, exist_ok=True)
                success, error = extract_zip(zip_path, extract_dir)
                if not success:
                    st.error(error)
                    return
                
                # Collect supported files
                supported_extensions = {'.csv', '.tsv', '.json'}
                if enable_recursive:
                    collected_files = collect_supported_files(extract_dir, supported_extensions)
                    input_paths.extend(collected_files)
                else:
                    # Non-recursive: only scan the top-level directory
                    for file in os.listdir(extract_dir):
                        file_path = os.path.join(extract_dir, file)
                        if os.path.isfile(file_path):
                            ext = os.path.splitext(file)[1].lower()
                            if ext in supported_extensions:
                                input_paths.append(file_path)
                
                if not input_paths:
                    st.error("No supported data files found in the uploaded archive.")
                    return
                
            # Save schema and config files
            schema_path = os.path.join(tmpdirname, "schema.json")
            with open(schema_path, 'wb') as f:
                f.write(schema_file.getbuffer())
                
            config_path = os.path.join(tmpdirname, "config.yaml")
            with open(config_path, 'wb') as f:
                f.write(config_file.getbuffer())
                
            # Save custom mappings if provided
            custom_mappings = None
            if custom_mapping_file:
                custom_mappings_path = os.path.join(tmpdirname, "custom_mapping.json")
                with open(custom_mappings_path, 'wb') as f:
                    f.write(custom_mapping_file.getbuffer())
                with open(custom_mappings_path, 'r') as f:
                    custom_mappings = json.load(f)
            
            # Load schema and configuration
            with open(schema_path, 'r') as f:
                schema = json.load(f)
            
            config = load_config(config_path)
            field_strategies = config.get('imputation_strategies', {})
            
            # Initialize OntologyMapper
            ontology_mapper = OntologyMapper(config_path=config_path)
            
            # Define output directory
            output_dir = os.path.join(tmpdirname, "reports")
            os.makedirs(output_dir, exist_ok=True)
            
            total_files = len(input_paths)
            progress_bar = st.progress(0)
            current_progress = 0
            progress_increment = 100 / total_files if total_files > 0 else 0

            st.header("Processing Results")
            
            # Create tabs for each file
            tab_labels = [os.path.basename(file_path) for file_path in input_paths]
            tabs = st.tabs(tab_labels) if tab_labels else []

            for idx, (file_path, tab) in enumerate(zip(input_paths, tabs)):
                file_name = os.path.basename(file_path)
                with tab:
                    st.subheader(f"Processing `{file_name}`")
                    log_activity(f"Processing file: {file_name}")

                    try:
                        # Process the file
                        result = process_file(
                            file_path=file_path,
                            schema=schema,
                            ontology_mapper=ontology_mapper,
                            unique_identifiers=unique_identifiers_list,
                            custom_mappings=custom_mappings,
                            impute_strategy=impute_strategy,
                            field_strategies=field_strategies,
                            output_dir=output_dir,
                            target_ontologies=ontologies_list
                        )

                        if result['status'] == 'Processed':
                            st.success(f"`{file_name}` processed successfully.")

                            # Read processed data
                            processed_data_path = os.path.join(output_dir, file_name)
                            try:
                                df = pd.read_csv(processed_data_path)
                            except Exception as e:
                                st.error(f"Failed to read processed data for `{file_name}`: {str(e)}")
                                log_activity(f"Failed to read processed data for {file_name}: {str(e)}", level='error')
                                continue

                            # Display visual summaries
                            st.subheader("Visual Summaries")
                            figs = create_visual_summary(df, output_image_path=None)
                            for fig_idx, fig in enumerate(figs):
                                st.plotly_chart(fig, use_container_width=True, key=f"{file_name}_plot_{fig_idx}")

                            # Generate QC report in memory
                            report_buffer = io.BytesIO()
                            generate_qc_report(
                                validation_results=result.get('validation_results', {}),
                                missing_data=result.get('missing_data', pd.Series()),
                                flagged_records_count=result.get('flagged_records_count', 0),
                                mapping_success_rates=result.get('mapping_success_rates', {}),
                                visualization_images=result.get('visualization_images', []),
                                impute_strategy=impute_strategy,
                                output_path_or_buffer=report_buffer
                            )
                            report_buffer.seek(0)

                            # Download buttons
                            st.download_button(
                                label=f"Download QC Report for `{file_name}` (PDF)",
                                data=report_buffer,
                                file_name=f"{os.path.splitext(file_name)[0]}_qc_report.pdf",
                                mime='application/pdf'
                            )

                            st.download_button(
                                label=f"Download Processed Data for `{file_name}` (CSV)",
                                data=df.to_csv(index=False).encode('utf-8'),
                                file_name=f"processed_{file_name}",
                                mime='text/csv'
                            )

                        elif result['status'] == 'Invalid':
                            st.warning(f"`{file_name}` failed validation: {result['error']}")
                        else:
                            st.error(f"`{file_name}` encountered an error: {result['error']}")

                    except Exception as e:
                        st.error(f"An error occurred while processing `{file_name}`: {str(e)}")
                        log_activity(f"Error processing file {file_path}: {str(e)}", level='error')

                    # Update progress bar
                    current_progress += progress_increment
                    progress_bar.progress(int(current_progress))

                    # Separator within tab
                    st.markdown("---")

            st.success("All files have been processed.")

if __name__ == '__main__':
    main()