import streamlit as st
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
from batch_processing import process_file
import shutil  # For deleting temporary directories
import yaml  # Needed for saving config
import warnings  # For suppressing warnings

def extract_zip(zip_path, extract_to):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            zip_file.extractall(extract_to)
        return True, None
    except zipfile.BadZipFile:
        return False, "The uploaded file is not a valid ZIP archive."
    except Exception as e:
        return False, f"An error occurred during ZIP extraction: {e}"

def collect_supported_files(directory, supported_extensions):
    collected_files = []
    for root, dirs, files in os.walk(directory):
        # Exclude '__MACOSX' directories and hidden files
        dirs[:] = [d for d in dirs if not d.startswith('__MACOSX')]
        for file in files:
            if file.startswith('._') or file.startswith('.DS_Store'):
                continue  # Skip macOS hidden files
            ext = os.path.splitext(file)[1].lower()
            if ext in supported_extensions:
                collected_files.append(os.path.join(root, file))
    return collected_files

def save_uploaded_file(uploaded_file):
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(uploaded_file.getvalue())
        return tmp_file.name

def main():
    setup_logging()
    st.set_page_config(page_title="PhenoQC - Phenotypic Data QC Toolkit", layout="wide")

    st.title("PhenoQC - Phenotypic Data Quality Control Toolkit")

    # Steps definition with order (merged Step 5 and 6)
    steps = [
        "Upload Config Files",
        "Upload Data Files",
        "Select Unique Identifier",
        "Select Ontologies & Impute",
        "Run QC and View Results"
    ]

    # Initialize session state at the very beginning
    if 'current_step' not in st.session_state:
        st.session_state.current_step = "Upload Config Files"
    if 'steps_completed' not in st.session_state:
        st.session_state.steps_completed = {step: False for step in steps}
    if 'processing_results' not in st.session_state:
        st.session_state['processing_results'] = []

    # Helper function to proceed to next step
    def proceed_to_step(step_name):
        st.session_state.current_step = step_name

    # Suppress the specific warning message about st.rerun()
    warnings.filterwarnings("ignore", message="Calling st.rerun() within a callback is a no-op.")

    # Sidebar navigation with improved UI
    with st.sidebar:
        st.header("Navigation")

        # Simplified CSS for sidebar buttons
        st.markdown(f"""
            <style>
                /* Style all buttons in the sidebar */
                div[data-testid="stSidebar"] button {{
                    width: 100% !important;
                    text-align: left !important;
                    padding: 10px 20px !important;
                    margin-bottom: 10px !important;
                    background-color: #4CAF50 !important; /* Default green background */
                    color: white !important;
                    border: none !important;
                    border-radius: 5px !important;
                    cursor: pointer !important;
                    font-size: 16px !important;
                }}
                /* Hover effect for buttons */
                div[data-testid="stSidebar"] button:hover {{
                    background-color: #45a049 !important; /* Darker green on hover */
                }}
                /* Style for the current active step using nth-child */
                div[data-testid="stSidebar"] button:nth-child({steps.index(st.session_state.get('current_step', steps[0])) + 1}) {{
                    background-color: #555555 !important; /* Grey background for active step */
                    cursor: default !important;
                }}
            </style>
        """, unsafe_allow_html=True)

        # Render buttons for each step
        for idx, title in enumerate(steps):
            if title != st.session_state.current_step:
                if st.button(title, key=title):
                    proceed_to_step(title)
            else:
                # Render a button without an on_click to make it non-clickable
                st.button(title, key=title, disabled=True)


    # Suppress other specific warnings if necessary
    warnings.filterwarnings('ignore', category=UnicodeWarning)

    # Step 1: Upload Configuration Files
    if st.session_state.current_step == "Upload Config Files":
        st.header("Step 1: Upload Configuration Files")
        config_col1, config_col2 = st.columns(2)
        
        # Flags to check if both files are uploaded
        config_loaded = False
        schema_loaded = False

        with config_col1:
            config_file = st.file_uploader(
                "Upload Configuration File (config.yaml)",
                type=["yaml", "yml"],
                key="config_file"
            )
            if config_file:
                try:
                    st.session_state['config'] = load_config(config_file)
                    st.session_state['available_ontologies'] = list(st.session_state['config'].get('ontologies', {}).keys())
                    st.success("Configuration file uploaded successfully.")
                    config_loaded = True
                except Exception as e:
                    st.error(f"Error loading configuration file: {e}")

        with config_col2:
            schema_file = st.file_uploader(
                "Upload JSON Schema File",
                type=["json"],
                key="schema_file"
            )
            if schema_file:
                try:
                    st.session_state['schema'] = json.load(schema_file)
                    st.success("JSON schema file uploaded successfully.")
                    schema_loaded = True
                except Exception as e:
                    st.error(f"Error loading JSON schema file: {e}")

        st.markdown("---")
        # Only proceed if both files are loaded
        if config_loaded and schema_loaded:
            st.session_state.steps_completed["Upload Config Files"] = True
            st.button("Proceed to Upload Data Files", on_click=proceed_to_step, args=("Upload Data Files",))
        else:
            st.session_state.steps_completed["Upload Config Files"] = False

    # Step 2: Upload Data Files
    elif st.session_state.current_step == "Upload Data Files":
        st.header("Step 2: Upload Data Files")
        data_source_option = st.radio(
            "Select Data Source",
            options=['Upload Files', 'Upload Directory (ZIP)'],
            key="data_source_option",
            on_change=lambda: st.session_state.pop('uploaded_files_list', None)
        )
        if data_source_option == 'Upload Files':
            st.session_state['data_source'] = 'files'
            uploaded_files = st.file_uploader(
                "Upload Phenotypic Data Files",
                type=["csv", "tsv", "json"],
                accept_multiple_files=True,
                key="uploaded_files_widget"
            )
            if uploaded_files:
                st.session_state['uploaded_files_list'] = uploaded_files
                st.session_state.steps_completed["Upload Data Files"] = True
        else:
            st.session_state['data_source'] = 'zip'
            uploaded_zip = st.file_uploader(
                "Upload Data Directory (ZIP Archive)",
                type=["zip"],
                key="uploaded_zip_widget"
            )
            # Checkbox for recursive scanning
            enable_recursive = st.checkbox(
                "Enable Recursive Directory Scanning",
                value=True,
                key="enable_recursive"
            )
            if uploaded_zip:
                st.session_state['uploaded_zip_file'] = uploaded_zip
                st.session_state.steps_completed["Upload Data Files"] = True
        st.markdown("---")
        if st.session_state.steps_completed["Upload Data Files"]:
            st.button("Proceed to Select Unique Identifier", on_click=proceed_to_step, args=("Select Unique Identifier",))

    # Step 3: Select Unique Identifier Columns
    elif st.session_state.current_step == "Select Unique Identifier":
        st.header("Step 3: Select Unique Identifier")
        sample_df = None

        if st.session_state['data_source'] == 'files':
            uploaded_files = st.session_state.get('uploaded_files_list', [])
            for uploaded_file in uploaded_files:
                try:
                    file_content = uploaded_file.getvalue()
                    ext = os.path.splitext(uploaded_file.name)[1].lower()
                    if ext == '.csv':
                        df = pd.read_csv(io.StringIO(file_content.decode('utf-8', 'ignore')), nrows=100)
                    elif ext == '.tsv':
                        df = pd.read_csv(io.StringIO(file_content.decode('utf-8', 'ignore')), sep='\t', nrows=100)
                    elif ext == '.json':
                        df = pd.read_json(io.StringIO(file_content.decode('utf-8', 'ignore')), lines=True, nrows=100)
                    else:
                        st.warning(f"Unsupported file extension: {ext}")
                        continue
                    if df.empty or df.columns.empty:
                        st.error(f"The file {uploaded_file.name} does not contain any data or columns.")
                        continue
                    sample_df = df
                    break  # Use the first valid file as a sample
                except Exception as e:
                    st.error(f"Error reading file {uploaded_file.name}: {str(e)}")
            if sample_df is None:
                st.error("No valid data files could be read. Please check your files and try again.")
                st.stop()
        else:
            uploaded_zip = st.session_state.get('uploaded_zip_file')
            if uploaded_zip:
                # Save the uploaded ZIP to a temporary file
                tmp_zip_path = save_uploaded_file(uploaded_zip)
                with zipfile.ZipFile(tmp_zip_path, 'r') as zip_ref:
                    supported_extensions = {'.csv', '.tsv', '.json'}
                    collected_files = [f for f in zip_ref.namelist() if os.path.splitext(f)[1].lower() in supported_extensions]
                    if collected_files:
                        for file_name in collected_files:
                            try:
                                with zip_ref.open(file_name) as f:
                                    file_content = f.read()
                                    ext = os.path.splitext(file_name)[1].lower()
                                    if ext == '.csv':
                                        df = pd.read_csv(io.StringIO(file_content.decode('utf-8', 'ignore')), nrows=100)
                                    elif ext == '.tsv':
                                        df = pd.read_csv(io.StringIO(file_content.decode('utf-8', 'ignore')), sep='\t', nrows=100)
                                    elif ext == '.json':
                                        df = pd.read_json(io.StringIO(file_content.decode('utf-8', 'ignore')), lines=True, nrows=100)
                                    else:
                                        st.warning(f"Unsupported file extension: {ext}")
                                        continue
                                    if df.empty or df.columns.empty:
                                        st.error(f"The file {file_name} does not contain any data or columns.")
                                        continue
                                    sample_df = df
                                    break  # Use the first valid file as a sample
                            except Exception as e:
                                st.error(f"Error reading file {file_name} in ZIP: {str(e)}")
                        if sample_df is None:
                            st.error("No valid data files could be read from the ZIP archive.")
                            st.stop()
                    else:
                        st.error("No supported data files found in the uploaded ZIP archive.")
                        st.stop()
                if sample_df is None:
                    st.error("No valid data files could be read from the ZIP archive. Please check your files and try again.")
                    st.stop()

        if sample_df is not None:
            st.subheader("Sample Data Preview")
            st.dataframe(sample_df.head(5))
            st.session_state['data_columns'] = sample_df.columns.tolist()
            previous_unique_identifiers = st.session_state.get('unique_identifiers_list', [])
            current_data_columns = st.session_state['data_columns']
            default_unique_identifiers = [uid for uid in previous_unique_identifiers if uid in current_data_columns]
            unique_identifiers = st.multiselect(
                "Select Unique Identifier Columns",
                options=current_data_columns,
                default=default_unique_identifiers,
                key="unique_identifiers_widget"
            )
            if unique_identifiers:
                st.session_state['unique_identifiers_list'] = unique_identifiers
                st.session_state.steps_completed["Select Unique Identifier"] = True
            else:
                st.warning("Please select at least one unique identifier column.")
                st.session_state.steps_completed["Select Unique Identifier"] = False

            # **New Input for Phenotype Column Name**
            st.subheader("Specify Phenotype Column")
            default_phenotype_column = st.session_state.get('phenotype_column', 'Phenotype')
            phenotype_column = st.selectbox(
                "Select the column name for Phenotypic Terms:",
                options=current_data_columns,
                index=current_data_columns.index(default_phenotype_column) if default_phenotype_column in current_data_columns else 0,
                key="phenotype_column_widget"
            )
            if phenotype_column:
                st.session_state['phenotype_column'] = phenotype_column
            else:
                st.warning("Please specify the column name for phenotypic terms.")
                st.session_state.steps_completed["Select Unique Identifier"] = False

        st.markdown("---")
        if st.session_state.steps_completed["Select Unique Identifier"]:
            st.button("Proceed to Select Ontologies & Impute", on_click=proceed_to_step, args=("Select Ontologies & Impute",))

    # Step 4: Select Ontologies and Imputation Strategy
    elif st.session_state.current_step == "Select Ontologies & Impute":
        st.header("Step 4: Select Ontologies & Impute")
        if st.session_state.get('available_ontologies'):
            default_ontologies = st.session_state.get('ontologies_selected_list', st.session_state['available_ontologies'])
            ontologies_selected = st.multiselect(
                "Select Ontologies to Map",
                options=st.session_state['available_ontologies'],
                default=default_ontologies,
                key="ontologies_selected_widget"
            )
            if ontologies_selected:
                st.session_state['ontologies_selected_list'] = ontologies_selected
                st.session_state.steps_completed["Select Ontologies & Impute"] = True
            else:
                st.error("Please select at least one ontology.")
                st.session_state.steps_completed["Select Ontologies & Impute"] = False
        else:
            st.error("No ontologies available in the configuration file.")
            st.stop()
        
        impute_strategy = st.selectbox(
            "Select Imputation Strategy",
            options=['mean', 'median', 'mode', 'knn', 'mice', 'svd', 'none'],
            index=st.session_state.get('impute_strategy_index', 0),
            key="impute_strategy_widget"
        )
        st.session_state['impute_strategy_value'] = impute_strategy
        st.session_state['impute_strategy_index'] = ['mean', 'median', 'mode', 'knn', 'mice', 'svd', 'none'].index(impute_strategy)
        
        custom_mapping_file = st.file_uploader(
            "Upload Custom Mapping File (Optional)",
            type=["json"],
            key="custom_mapping_widget"
        )
        if custom_mapping_file:
            try:
                st.session_state['custom_mappings_data'] = json.load(custom_mapping_file)
                st.success("Custom mapping file uploaded successfully.")
            except Exception as e:
                st.error(f"Error loading custom mapping file: {e}")
        else:
            st.session_state['custom_mappings_data'] = None
        st.markdown("---")
        if st.session_state.steps_completed["Select Ontologies & Impute"]:
            st.button("Proceed to Run QC and View Results", on_click=proceed_to_step, args=("Run QC and View Results",))

    # Step 5: Run QC and View Results (merged step)
    elif st.session_state.current_step == "Run QC and View Results":
        st.header("Step 5: Run Quality Control and View Results")
        if not st.session_state.steps_completed["Run QC and View Results"]:
            # If processing not done, show the Start Processing button
            if st.button("Start Processing", key="start_processing_button"):
                with st.spinner("Processing..."):
                    try:
                        # Create temporary directory if not already created
                        if 'tmpdirname' not in st.session_state:
                            st.session_state.tmpdirname = tempfile.mkdtemp()
                        tmpdirname = st.session_state.tmpdirname
                        input_paths = []
                        schema_path = os.path.join(tmpdirname, "schema.json")
                        with open(schema_path, 'w') as f:
                            json.dump(st.session_state['schema'], f)
                        config_path = os.path.join(tmpdirname, "config.yaml")
                        with open(config_path, 'w') as f:
                            yaml.dump(st.session_state['config'], f)
                        # Save custom mappings if provided
                        if st.session_state['custom_mappings_data']:
                            custom_mappings_path = os.path.join(tmpdirname, "custom_mapping.json")
                            with open(custom_mappings_path, 'w') as f:
                                json.dump(st.session_state['custom_mappings_data'], f)
                        else:
                            custom_mappings_path = None
                        # Handle data files
                        if st.session_state['data_source'] == 'files':
                            uploaded_files = st.session_state.get('uploaded_files_list', [])
                            for uploaded_file in uploaded_files:
                                file_path = os.path.join(tmpdirname, uploaded_file.name)
                                with open(file_path, 'wb') as f:
                                    f.write(uploaded_file.getbuffer())
                                input_paths.append(file_path)
                        else:
                            # Extract ZIP archive
                            uploaded_zip = st.session_state.get('uploaded_zip_file')
                            if uploaded_zip:
                                # Save the uploaded ZIP to a temporary file
                                tmp_zip_path = save_uploaded_file(uploaded_zip)
                                extract_dir = os.path.join(tmpdirname, "extracted")
                                os.makedirs(extract_dir, exist_ok=True)
                                success, error = extract_zip(tmp_zip_path, extract_dir)
                                if not success:
                                    st.error(error)
                                    st.stop()
                                # Collect supported files
                                supported_extensions = {'.csv', '.tsv', '.json'}
                                collected_files = []
                                # Always use recursive scanning for better reliability
                                collected_files = collect_supported_files(extract_dir, supported_extensions)
                                st.info(f"Collected {len(collected_files)} files.")
                                input_paths.extend(collected_files)
                        if not input_paths:
                            st.error("No data files found to process.")
                            st.stop()
                       
                        # Initialize OntologyMapper
                        ontology_mapper = OntologyMapper(config_path=config_path)
                        # Load configuration
                        config = st.session_state['config']
                        field_strategies = config.get('imputation_strategies', {})
                        # Define output directory
                        output_dir = os.path.join(tmpdirname, "reports")
                        os.makedirs(output_dir, exist_ok=True)
                        total_files = len(input_paths)
                        progress_bar = st.progress(0)
                        current_progress = 0
                        progress_increment = 100 / total_files if total_files > 0 else 0
                        st.session_state['processing_results'] = []
                        for idx, file_path in enumerate(input_paths):
                            file_name = os.path.basename(file_path)
                            st.write(f"Processing {file_name}...")
                            try:
                                result = process_file(
                                    file_path=file_path,
                                    schema=st.session_state['schema'],
                                    ontology_mapper=ontology_mapper,
                                    unique_identifiers=st.session_state.get('unique_identifiers_list', []),
                                    custom_mappings=st.session_state.get('custom_mappings_data'),
                                    impute_strategy=st.session_state.get('impute_strategy_value'),
                                    field_strategies=field_strategies,
                                    output_dir=output_dir,
                                    target_ontologies=st.session_state.get('ontologies_selected_list', []),
                                    phenotype_column=st.session_state.get('phenotype_column', 'Phenotype')  # Pass phenotype_column
                                )

                                # Get the actual processed file path from result
                                processed_data_path = result.get('processed_file_path')
                                if processed_data_path and os.path.exists(processed_data_path):
                                    st.success(f"{file_name} processed successfully.")
                                else:
                                    st.warning(f"Processed data file not found for {file_name}.")
                                st.session_state['processing_results'].append((file_name, result, output_dir))
                            except Exception as e:
                                st.error(f"An error occurred while processing {file_name}: {e}")
                                continue
                            current_progress += progress_increment
                            progress_bar.progress(int(current_progress))
                        st.success("Processing completed!")
                        st.session_state.steps_completed["Run QC and View Results"] = True
                    except Exception as e:
                        st.error(f"An error occurred during processing: {e}")


        if st.session_state.steps_completed.get("Run QC and View Results", False):
            st.header("Results")
            if 'processing_results' in st.session_state and st.session_state['processing_results']:
                # Create tabs for each file
                tab_labels = [os.path.basename(file_name) for file_name, _, _ in st.session_state['processing_results']]
                tabs = st.tabs(tab_labels)

                for (file_name, result, output_dir), tab in zip(st.session_state['processing_results'], tabs):
                    with tab:
                        st.subheader(f"Results for {file_name}")

                        # --- Status checks ---
                        if result['status'] == 'Processed':
                            st.success("File processed successfully.")
                        elif result['status'] == 'ProcessedWithWarnings':
                            st.warning(
                                "File processed, but there were schema or integrity warnings. "
                                "Please see details below."
                            )
                        elif result['status'] == 'Invalid':
                            st.warning(f"File failed validation: {result['error']}")
                        else:
                            st.error(f"An error occurred: {result['error']}")

                        # --- If processed or partially processed, show details ---
                        processed_data_path = result.get('processed_file_path')
                        if processed_data_path and os.path.exists(processed_data_path):
                            try:
                                df = pd.read_csv(processed_data_path)
                                st.write("### Sample of Processed Data:")
                                st.dataframe(df.head(5))

                                # -- Display validation summaries in collapsible sections --
                                validation_res = result.get('validation_results', {})

                                # 1) Format Validation
                                if validation_res.get('Format Validation') is False:
                                    st.error(
                                        "Some records do not match the JSON schema. "
                                        "See 'Integrity Issues' section below."
                                    )

                                # 2) Integrity Issues
                                integrity_df = validation_res.get("Integrity Issues")
                                if isinstance(integrity_df, pd.DataFrame) and not integrity_df.empty:
                                    with st.expander("Integrity Issues", expanded=False):
                                        st.error("Rows that failed validation or had data-type/constraint issues:")
                                        st.dataframe(integrity_df.head(50))

                                # 3) Duplicate Records
                                duplicates_df = validation_res.get("Duplicate Records")
                                if isinstance(duplicates_df, pd.DataFrame) and not duplicates_df.empty:
                                    with st.expander("Duplicate Records", expanded=False):
                                        st.warning("The following rows appear more than once based on unique identifiers:")
                                        st.dataframe(duplicates_df)

                                # 4) Conflicting Records
                                conflicts_df = validation_res.get("Conflicting Records")
                                if isinstance(conflicts_df, pd.DataFrame) and not conflicts_df.empty:
                                    with st.expander("Conflicting Records", expanded=False):
                                        st.warning("Within duplicated rows, some columns have conflicting data:")
                                        st.dataframe(conflicts_df)

                                # 5) Anomalies Detected
                                anomalies_df = validation_res.get("Anomalies Detected")
                                if isinstance(anomalies_df, pd.DataFrame) and not anomalies_df.empty:
                                    with st.expander("Anomalies Detected", expanded=False):
                                        st.warning(
                                            "Potential outliers based on a simple Z-score approach (|z|>3). "
                                            "Rows listed here may need review."
                                        )
                                        st.dataframe(anomalies_df)

                                # 6) Visualization
                                st.write("### Visual Summaries")
                                figs = create_visual_summary(
                                    df,
                                    phenotype_column=st.session_state.get('phenotype_column', 'Phenotype'),
                                    output_image_path=None
                                )
                                num_figs = len(figs)
                                if num_figs > 0:
                                    # Arrange graphs in two columns
                                    cols = st.columns(2)
                                    for i, fig in enumerate(figs):
                                        with cols[i % 2]:
                                            st.plotly_chart(fig, use_container_width=True, key=f"{file_name}_plot_{i}")

                                # 7) Quality Scores
                                quality_scores = result.get('quality_scores', {})
                                if quality_scores:
                                    st.write("### Quality Scores")
                                    for score_name, score_val in quality_scores.items():
                                        st.write(f"- **{score_name}**: {score_val:.2f}%")

                                # -- Optionally let user download the final QC report (PDF) & processed data --
                                st.write("### Downloads")
                                report_buffer = io.BytesIO()
                                generate_qc_report(
                                    validation_results=validation_res,
                                    missing_data=result.get('missing_data', pd.Series()),
                                    flagged_records_count=result.get('flagged_records_count', 0),
                                    mapping_success_rates=result.get('mapping_success_rates', {}),
                                    visualization_images=result.get('visualization_images', []),
                                    impute_strategy=st.session_state.get('impute_strategy_value'),
                                    quality_scores=quality_scores,
                                    output_path_or_buffer=report_buffer,
                                    report_format='pdf'
                                )
                                report_buffer.seek(0)

                                st.download_button(
                                    label=f"Download QC Report for {file_name} (PDF)",
                                    data=report_buffer,
                                    file_name=f"{os.path.splitext(file_name)[0]}_qc_report.pdf",
                                    mime='application/pdf'
                                )

                                # Processed data
                                st.download_button(
                                    label=f"Download Processed Data for {file_name} (CSV)",
                                    data=df.to_csv(index=False).encode('utf-8'),
                                    file_name=f"processed_{file_name}",
                                    mime='text/csv'
                                )

                            except Exception as ex:
                                st.error(f"Failed to read processed data for {file_name}: {str(ex)}")
                        else:
                            st.error("Processed data file not found or no partial output available.")

            else:
                st.info("No processing results available.")

if __name__ == '__main__':
    main()
