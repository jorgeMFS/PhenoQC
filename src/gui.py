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
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
from validation import DataValidator  # or wherever your validation code is

def extract_zip(zip_path, extract_to):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            zip_file.extractall(extract_to)
        return True, None
    except zipfile.BadZipFile:
        return False, "The uploaded file is not a valid ZIP archive."
    except Exception as e:
        return False, f"An error occurred during ZIP extraction: {e}"

def on_editor_change():
    edited_df = st.session_state["editor_key"]
    st.session_state["edited_duplicates"] = edited_df
    # (Optional hook) Here, you can add code to handle the edited DataFrame.


def display_editable_grid_with_highlighting(df: pd.DataFrame,
                                            invalid_mask: pd.DataFrame,
                                            allow_edit: bool = True) -> pd.DataFrame:
    """
    Simple, functional editable grid with error highlighting and scrollable width.
    Only editable if allow_edit=True.
    """
    df = df.reset_index(drop=True)  # ensure a clean, 0-based index

    # Intersect the columns so we don’t get KeyErrors
    common_cols = df.columns.intersection(invalid_mask.columns)
    invalid_mask = invalid_mask[common_cols].reindex(df.index, fill_value=False)

    # Create hidden "_isInvalid" columns
    for col in common_cols:
        is_invalid_col = f"{col}_isInvalid"
        if is_invalid_col not in df.columns:
            df[is_invalid_col] = invalid_mask[col].astype(bool)

    # Build the AgGrid config
    gb = GridOptionsBuilder.from_dataframe(df)

    for col in df.columns:
        if col.endswith("_isInvalid"):
            # Hide the helper column
            gb.configure_column(col, hide=True)
        else:
            # If we have a _isInvalid counterpart, set cellStyle
            is_invalid_col = col + "_isInvalid"
            if is_invalid_col in df.columns:
                cell_style_jscode = JsCode(f"""
                    function(params) {{
                        if (params.data['{is_invalid_col}'] === true) {{
                            return {{'backgroundColor': '#fff5f5'}};
                        }}
                        return null;
                    }}
                """)
                gb.configure_column(col, 
                                    editable=allow_edit, 
                                    cellStyle=cell_style_jscode, 
                                    minWidth=200)
            else:
                gb.configure_column(col, editable=allow_edit, minWidth=200)

    grid_options = gb.build()
    grid_options['defaultColDef'] = {
        'resizable': True,
        'sortable': True,
        'minWidth': 200
    }

    # Render the grid
    grid_response = AgGrid(
        df,
        gridOptions=grid_options,
        data_return_mode='AS_INPUT',
        update_mode='MODEL_CHANGED',
        fit_columns_on_grid_load=False,
        height=min(400, len(df) * 35 + 40),
        allow_unsafe_jscode=True,
        theme='streamlit',
        custom_css={
            ".ag-header-cell": {
                "background-color": "#f0f0f0",
                "font-weight": "500",
                "padding": "8px",
                "height": "48px !important",
                "line-height": "1.2 !important",
                "white-space": "normal !important"
            },
            ".ag-cell": {
                "padding-left": "8px",
                "padding-right": "8px"
            }
        }
    )

    edited_df = pd.DataFrame(grid_response['data'])
    cols_to_drop = [c for c in edited_df.columns if c.endswith("_isInvalid")]
    edited_df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    return edited_df


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
        # ----------------------------------------------------
    # 5) Run QC and View Results (merged step)
    # ----------------------------------------------------
    elif st.session_state.current_step == "Run QC and View Results":
        st.header("Step 5: Run Quality Control and View Results")

        # If not processed yet, show the "Start Processing" button
        if not st.session_state.steps_completed["Run QC and View Results"]:
            if st.button("Start Processing", key="start_processing_button"):
                with st.spinner("Processing..."):
                    try:
                        if 'tmpdirname' not in st.session_state:
                            st.session_state.tmpdirname = tempfile.mkdtemp()

                        tmpdirname = st.session_state.tmpdirname
                        input_paths = []

                        # Save schema.json
                        schema_path = os.path.join(tmpdirname, "schema.json")
                        with open(schema_path, 'w') as f:
                            json.dump(st.session_state['schema'], f)

                        # Save config.yaml
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

                        # Collect input file paths
                        if st.session_state['data_source'] == 'files':
                            uploaded_files = st.session_state.get('uploaded_files_list', [])
                            for uploaded_file in uploaded_files:
                                file_path = os.path.join(tmpdirname, uploaded_file.name)
                                with open(file_path, 'wb') as f:
                                    f.write(uploaded_file.getbuffer())
                                input_paths.append(file_path)
                        else:
                            # For ZIP
                            uploaded_zip = st.session_state.get('uploaded_zip_file')
                            if uploaded_zip:
                                tmp_zip_path = save_uploaded_file(uploaded_zip)
                                extract_dir = os.path.join(tmpdirname, "extracted")
                                os.makedirs(extract_dir, exist_ok=True)
                                success, error = extract_zip(tmp_zip_path, extract_dir)
                                if not success:
                                    st.error(error)
                                    st.stop()
                                supported_extensions = {'.csv', '.tsv', '.json'}
                                collected_files = collect_supported_files(extract_dir, supported_extensions)
                                st.info(f"Collected {len(collected_files)} files.")
                                input_paths.extend(collected_files)

                        if not input_paths:
                            st.error("No data files found to process.")
                            st.stop()

                        # Initialize OntologyMapper
                        ontology_mapper = OntologyMapper(config_path=config_path)

                        config = st.session_state['config']
                        field_strategies = config.get('imputation_strategies', {})

                        output_dir = os.path.join(tmpdirname, "reports")
                        os.makedirs(output_dir, exist_ok=True)

                        total_files = len(input_paths)
                        progress_bar = st.progress(0)
                        current_progress = 0
                        progress_increment = 100 / total_files if total_files > 0 else 0

                        # Clear previous results
                        st.session_state['processing_results'] = []

                        # Run QC on each file
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
                                    phenotype_column=st.session_state.get('phenotype_column', 'Phenotype')
                                )
                                st.session_state['processing_results'].append((file_name, result, output_dir))

                                # Quick status
                                if result['status'] == 'Processed':
                                    st.success(f"{file_name} processed successfully.")
                                elif result['status'] == 'ProcessedWithWarnings':
                                    st.warning(f"{file_name} processed with warnings.")
                                elif result['status'] == 'Invalid':
                                    st.warning(f"{file_name} validation failed: {result['error']}")
                                else:
                                    st.error(f"{file_name} error: {result['error']}")

                            except Exception as e:
                                st.error(f"Error processing {file_name}: {e}")

                            current_progress += progress_increment
                            progress_bar.progress(int(current_progress))

                        st.success("Processing completed!")
                        st.session_state.steps_completed["Run QC and View Results"] = True

                    except Exception as e:
                        st.error(f"An error occurred during processing: {e}")

        # If we have processed results, show them
        if st.session_state.steps_completed.get("Run QC and View Results", False):
            st.header("Results")
            if 'processing_results' in st.session_state and st.session_state['processing_results']:
                tab_labels = [os.path.basename(fname) for fname, _, _ in st.session_state['processing_results']]
                tabs = st.tabs(tab_labels)

                for (file_name, result, output_dir), tab in zip(st.session_state['processing_results'], tabs):
                    with tab:
                        st.subheader(f"Results for {file_name}")

                        file_status = result['status']
                        if file_status == 'Processed':
                            st.success("File processed successfully.")
                        elif file_status == 'ProcessedWithWarnings':
                            st.warning("File processed with warnings or schema violations.")
                        elif file_status == 'Invalid':
                            st.warning(f"File failed validation: {result['error']}")
                        else:
                            st.error(f"File encountered an error: {result['error']}")

                        processed_data_path = result.get('processed_file_path')
                        if not processed_data_path or not os.path.exists(processed_data_path):
                            st.error("Processed data file not found. No partial output available.")
                            continue

                        try:
                            df = pd.read_csv(processed_data_path)
                        except Exception as ex:
                            st.error(f"Failed to read processed data: {str(ex)}")
                            continue

                        st.write("### Sample of Processed Data:")
                        st.dataframe(df.head(5))

                        # Build summary
                        validation_res = result.get('validation_results', {})
                        summary_text = []

                        if validation_res.get('Format Validation') is False:
                            summary_text.append("- Some rows did NOT match the JSON schema.")
                        else:
                            summary_text.append("- All rows appear to match the JSON schema (or partial).")

                        duplicates_df = validation_res.get("Duplicate Records")
                        if isinstance(duplicates_df, pd.DataFrame) and not duplicates_df.empty:
                            summary_text.append(f"- Found **{len(duplicates_df.drop_duplicates())}** duplicate rows.")
                        else:
                            summary_text.append("- No duplicates found.")

                        conflicts_df = validation_res.get("Conflicting Records")
                        if isinstance(conflicts_df, pd.DataFrame) and not conflicts_df.empty:
                            summary_text.append(f"- Found **{len(conflicts_df.drop_duplicates())}** conflicting records.")
                        else:
                            summary_text.append("- No conflicting records found.")

                        integrity_df = validation_res.get("Integrity Issues")
                        if isinstance(integrity_df, pd.DataFrame) and not integrity_df.empty:
                            summary_text.append(f"- Found **{len(integrity_df.drop_duplicates())}** integrity issues.")
                        else:
                            summary_text.append("- No integrity issues found.")

                        anomalies_df = validation_res.get("Anomalies Detected")
                        if isinstance(anomalies_df, pd.DataFrame) and not anomalies_df.empty:
                            summary_text.append(f"- Found **{len(anomalies_df.drop_duplicates())}** anomalies.")
                        else:
                            summary_text.append("- No anomalies detected.")

                        st.info("**Summary of Key Findings**\n\n" + "\n".join(summary_text))

                        # =======================
                        #  ONLY if there's exactly one file
                        #  AND if Format Validation = False
                        #  => show in-place editing
                        # =======================
                        num_files_processed = len(st.session_state['processing_results'])
                        format_valid = validation_res.get("Format Validation", True)

                        if num_files_processed == 1 and not format_valid:
                            st.write("### In-Place Editing & Re-validation")
                            invalid_mask = validation_res.get("Invalid Mask", pd.DataFrame())
                            if invalid_mask.empty:
                                st.write("No invalid cells found (or no mask returned).")
                                st.write("Feel free to edit the data below anyway.")
                                invalid_mask = pd.DataFrame(False, index=df.index, columns=df.columns)

                            key_prefix = file_name.replace('.', '_')
                            if f"{key_prefix}_df" not in st.session_state:
                                st.session_state[f"{key_prefix}_df"] = df.copy()
                            if f"{key_prefix}_mask" not in st.session_state:
                                st.session_state[f"{key_prefix}_mask"] = invalid_mask.copy()

                            # We allow editing
                            editable_df = display_editable_grid_with_highlighting(
                                st.session_state[f"{key_prefix}_df"].copy(),
                                st.session_state[f"{key_prefix}_mask"].copy(),
                                allow_edit=True
                            )

                            # Button to re-validate
                            if st.button(f"Re-Validate Data ({file_name})"):
                                edited_df = editable_df.copy()
                                edited_df.index = st.session_state[f"{key_prefix}_df"].index
                                st.session_state[f"{key_prefix}_df"] = edited_df

                                # Re-run validations on the edited data
                                schema = st.session_state['schema']
                                unique_ids = st.session_state.get('unique_identifiers_list', [])
                                validator = DataValidator(edited_df, schema, unique_ids)
                                results2 = validator.run_all_validations()
                                new_mask = results2["Invalid Mask"]
                                st.session_state[f"{key_prefix}_mask"] = new_mask

                                if not results2["Format Validation"]:
                                    st.warning("Some rows do not match the schema after re-validation.")
                                else:
                                    st.success("All rows appear valid after re-validation!")

                                st.experimental_rerun()
                        else:
                            # If multiple files or the validation didn't fail, we skip editing
                            st.write("**No in-place editing is available** because either:")
                            st.write("- Multiple files were processed, **OR**")
                            st.write("- The data already passes validation.")
                            st.write("You can still review the data above or re-run with a single file.")
                        
                        # ================
                        # Visual Summaries
                        # ================
                        st.write("### Visual Summaries")
                        figs = create_visual_summary(
                            df,
                            phenotype_column=st.session_state.get('phenotype_column', 'Phenotype'),
                            output_image_path=None
                        )
                        if figs:
                            cols = st.columns(2)
                            for i, fig in enumerate(figs):
                                with cols[i % 2]:
                                    st.plotly_chart(fig, use_container_width=True, key=f"{file_name}_plot_{i}")

                        # ======================
                        # Quality Scores + Downloads
                        # ======================
                        st.write("### Quality Scores")
                        q_scores = result.get('quality_scores', {})
                        for score_name, score_val in q_scores.items():
                            st.write(f"- **{score_name}**: {score_val:.2f}%")

                        st.write("### Downloads")
                        report_buffer = io.BytesIO()
                        generate_qc_report(
                            validation_results=validation_res,
                            missing_data=result.get('missing_data', pd.Series()),
                            flagged_records_count=result.get('flagged_records_count', 0),
                            mapping_success_rates=result.get('mapping_success_rates', {}),
                            visualization_images=result.get('visualization_images', []),
                            impute_strategy=st.session_state.get('impute_strategy_value'),
                            quality_scores=q_scores,
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

                        st.download_button(
                            label=f"Download Processed Data for {file_name} (CSV)",
                            data=df.to_csv(index=False).encode('utf-8'),
                            file_name=f"processed_{file_name}",
                            mime='text/csv'
                        )
            else:
                st.info("No processing results available.")

if __name__ == '__main__':
    main()