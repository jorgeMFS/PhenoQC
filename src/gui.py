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
from utils.ontology_utils import suggest_ontologies
import glob
import numpy as np

def preserve_original_format_and_save(df_in_memory, original_filename, out_dir):
    base, ext = os.path.splitext(original_filename)
    ext = ext.lower()

    out_path = os.path.join(out_dir, original_filename)  # Keep original name

    if ext == '.csv':
        # save as CSV
        df_in_memory.to_csv(out_path, index=False)

    elif ext == '.tsv':
        # save as tab-delimited
        df_in_memory.to_csv(out_path, sep='\t', index=False)

    elif ext == '.json':
        # JSON can be tricky: if you know it was array-of-objects, use lines=False
        # if you know it was NDJSON, use lines=True. Adjust orient as needed.
        df_in_memory.to_json(out_path, orient='records', lines=False, indent=2)

    else:
        # fallback or skip
        raise ValueError(f"Unsupported extension: {ext}")

    return out_path

def extract_zip(zip_path, extract_to):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            # Filter out macOS-specific files and directories
            members = [f for f in zip_file.namelist() 
                      if not f.startswith('__MACOSX/') 
                      and not f.startswith('._')
                      and not f.endswith('.DS_Store')]
            # Extract only the filtered files
            for member in members:
                zip_file.extract(member, extract_to)
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
            # Skip macOS metadata files and hidden files
            if file.startswith('._') or file.startswith('.DS_Store'):
                continue
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
        st.session_state.current_step = steps[0]
        st.session_state.steps_completed = {step: False for step in steps}
    if 'processing_results' not in st.session_state:
        st.session_state['processing_results'] = []
        st.session_state['custom_mappings_data'] = None
    # Helper function to proceed to next step
    def proceed_to_step(step_name):
        if step_name in steps:  # Verify step exists
            st.session_state.current_step = step_name
        else:
            st.error(f"Invalid step name: {step_name}")

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

    ##############################################################
    # Step 2: Upload Data Files
    ##############################################################
    elif st.session_state.current_step == "Upload Data Files":
        st.header("Step 2: Upload Data Files")
        data_source_option = st.radio(
            "Select Data Source",
            options=['Upload Files', 'Upload Directory (ZIP)'],
            key="data_source_option",
            on_change=lambda: st.session_state.pop('uploaded_files_list', None)
        )

        # 1) If user chooses to upload individual files
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

            # Once user is done uploading, read them all into st.session_state["multi_dfs"]
            if st.session_state.steps_completed["Upload Data Files"]:
                st.session_state["multi_dfs"] = {}
                union_cols = set()

                # Read each file
                uploaded_files = st.session_state.get('uploaded_files_list', [])
                for uploaded_file in uploaded_files:
                    file_content = uploaded_file.getvalue()
                    ext = os.path.splitext(uploaded_file.name)[1].lower()
                    df = None
                    try:
                        if ext == '.csv':
                            df = pd.read_csv(io.StringIO(file_content.decode('utf-8','ignore')))
                        elif ext == '.tsv':
                            df = pd.read_csv(io.StringIO(file_content.decode('utf-8','ignore')), sep='\t')
                        elif ext == '.json':
                            try:
                                df = pd.read_json(io.StringIO(file_content.decode('utf-8','ignore')), lines=True)
                            except ValueError:
                                df = pd.read_json(io.StringIO(file_content.decode('utf-8','ignore')), lines=False)
                        else:
                            st.warning(f"Skipped unsupported extension for {uploaded_file.name}")

                        if df is not None and not df.empty:
                            st.session_state["multi_dfs"][uploaded_file.name] = df
                            union_cols.update(df.columns)
                    except Exception as e:
                        st.error(f"Could not read {uploaded_file.name}: {e}")

                # Store the union of columns
                st.session_state["union_of_columns"] = list(union_cols)

        # 2) If user chooses to upload a ZIP
        else:
            st.session_state['data_source'] = 'zip'
            uploaded_zip = st.file_uploader(
                "Upload Data Directory (ZIP Archive)",
                type=["zip"],
                key="uploaded_zip_widget"
            )
            enable_recursive = st.checkbox(
                "Enable Recursive Directory Scanning",
                value=True,
                key="enable_recursive"
            )

            if uploaded_zip:
                # Create a temp directory (if not already present)
                if 'tmpdirname' not in st.session_state:
                    st.session_state.tmpdirname = tempfile.mkdtemp()

                zip_path = save_uploaded_file(uploaded_zip)
                extract_dir = os.path.join(st.session_state.tmpdirname, "extracted")

                # Clear old extractions
                if os.path.exists(extract_dir):
                    shutil.rmtree(extract_dir)
                os.makedirs(extract_dir, exist_ok=True)

                success, error = extract_zip(zip_path, extract_dir)
                if not success:
                    st.error(error)
                    st.stop()

                st.session_state['extracted_files_list'] = []
                if enable_recursive:
                    for root, dirs, files in os.walk(extract_dir):
                        for f in files:
                            ext = os.path.splitext(f)[1].lower()
                            if ext in {'.csv', '.tsv', '.json'}:
                                st.session_state['extracted_files_list'].append(os.path.join(root, f))
                else:
                    # Just top-level
                    top_files = os.listdir(extract_dir)
                    for f in top_files:
                        ext = os.path.splitext(f)[1].lower()
                        if ext in {'.csv', '.tsv', '.json'}:
                            st.session_state['extracted_files_list'].append(os.path.join(extract_dir, f))

                if st.session_state['extracted_files_list']:
                    st.success(f"ZIP extracted. Found {len(st.session_state['extracted_files_list'])} supported files.")
                else:
                    st.warning("ZIP extracted but found no .csv/.tsv/.json inside.")

                st.session_state['uploaded_zip_file'] = uploaded_zip
                st.session_state.steps_completed["Upload Data Files"] = True

                # Now read them all into multi_dfs
                st.session_state["multi_dfs"] = {}
                union_cols = set()

                extracted_files = st.session_state.get('extracted_files_list', [])
                for fpath in extracted_files:
                    ext = os.path.splitext(fpath)[1].lower()
                    df = None
                    try:
                        with open(fpath, 'rb') as f:
                            content = f.read()
                        if ext == '.csv':
                            df = pd.read_csv(io.StringIO(content.decode('utf-8','ignore')))
                        elif ext == '.tsv':
                            df = pd.read_csv(io.StringIO(content.decode('utf-8','ignore')), sep='\t')
                        elif ext == '.json':
                            try:
                                df = pd.read_json(io.StringIO(content.decode('utf-8','ignore')), lines=True)
                            except ValueError:
                                df = pd.read_json(io.StringIO(content.decode('utf-8','ignore')), lines=False)
                        if df is not None and not df.empty:
                            fname = os.path.basename(fpath)
                            st.session_state["multi_dfs"][fname] = df
                            union_cols.update(df.columns)
                    except Exception as e:
                        st.error(f"Error reading {os.path.basename(fpath)}: {e}")

                st.session_state["union_of_columns"] = list(union_cols)

                if st.session_state["multi_dfs"]:
                    st.info(f"Loaded {len(st.session_state['multi_dfs'])} valid data files from ZIP.")
                    st.info(f"Union of columns: {len(st.session_state['union_of_columns'])} total columns found.")
                else:
                    st.warning("No valid data loaded from ZIP.")

        st.markdown("---")
        if st.session_state.steps_completed["Upload Data Files"]:
            st.button("Proceed to Select Unique Identifier", on_click=proceed_to_step, args=("Select Unique Identifier",))

    ######################################################################
    # Step 3: Column Mapping and Ontology Configuration — COMPLETE REPLACEMENT
    ######################################################################
    elif st.session_state.current_step == "Select Unique Identifier":
        st.header("Step 3: Configure Data Mapping")

        # ----------------------------------------------------------------
        # 1) If 'sample_df' not in session, attempt to load the first valid
        #    CSV/TSV/JSON from either 'files' or 'zip' mode, for preview only.
        # ----------------------------------------------------------------
        if 'sample_df' not in st.session_state:
            sample_df_loaded = False
            if st.session_state['data_source'] == 'files':
                # If user uploaded files individually
                uploaded_files = st.session_state.get('uploaded_files_list', [])
                for uploaded_file in uploaded_files:
                    try:
                        file_content = uploaded_file.getvalue()
                        ext = os.path.splitext(uploaded_file.name)[1].lower()
                        if ext == '.csv':
                            st.session_state['sample_df'] = pd.read_csv(io.StringIO(file_content.decode('utf-8', 'ignore')))
                            sample_df_loaded = True
                            break
                        elif ext == '.tsv':
                            st.session_state['sample_df'] = pd.read_csv(io.StringIO(file_content.decode('utf-8', 'ignore')), sep='\t')
                            sample_df_loaded = True
                            break
                        elif ext == '.json':
                            try:
                                st.session_state['sample_df'] = pd.read_json(io.StringIO(file_content.decode('utf-8', 'ignore')), lines=True)
                            except ValueError:
                                st.session_state['sample_df'] = pd.read_json(io.StringIO(file_content.decode('utf-8', 'ignore')), lines=False)
                            sample_df_loaded = True
                            break
                    except Exception as e2:
                        st.error(f"Error reading file {uploaded_file.name}: {str(e2)}")

                if not sample_df_loaded:
                    st.warning("No valid CSV/TSV/JSON file could be loaded from the uploaded files.")

            else:
                # data_source == 'zip'
                # We rely on st.session_state['extracted_files_list'], built in Step 2
                extracted_files = st.session_state.get('extracted_files_list', [])
                if not extracted_files:
                    st.warning("No extracted .csv/.tsv/.json files to load. Please re-check your ZIP.")
                else:
                    for fpath in extracted_files:
                        try:
                            ext = os.path.splitext(fpath)[1].lower()
                            with open(fpath, 'rb') as file_in:
                                content = file_in.read()
                            if ext == '.csv':
                                st.session_state['sample_df'] = pd.read_csv(io.StringIO(content.decode('utf-8', 'ignore')))
                                sample_df_loaded = True
                                break
                            elif ext == '.tsv':
                                st.session_state['sample_df'] = pd.read_csv(io.StringIO(content.decode('utf-8', 'ignore')), sep='\t')
                                sample_df_loaded = True
                                break
                            elif ext == '.json':
                                try:
                                    st.session_state['sample_df'] = pd.read_json(io.StringIO(content.decode('utf-8','ignore')), lines=True)
                                except ValueError:
                                    st.session_state['sample_df'] = pd.read_json(io.StringIO(content.decode('utf-8','ignore')), lines=False)
                                sample_df_loaded = True
                                break
                        except Exception as e3:
                            st.error(f"Error reading extracted file {os.path.basename(fpath)}: {str(e3)}")

                    if not sample_df_loaded:
                        st.warning("No valid CSV/TSV/JSON found among extracted ZIP files.")

        sample_df = st.session_state.get('sample_df')
        if sample_df is None or sample_df.empty:
            st.error("Could not load sample data. Please try uploading your files again.")
            st.stop()

        # -------------------------------------------------------------------------
        # 2) For mapping: show the UNION of columns from all dataframes,
        #    so user sees ALL possible columns from all uploaded files.
        # -------------------------------------------------------------------------
        if "union_of_columns" in st.session_state and st.session_state["union_of_columns"]:
            all_columns = st.session_state["union_of_columns"]
        else:
            # fallback if union_of_columns not set
            all_columns = list(sample_df.columns)

        st.subheader("A) Select Columns for Ontology Mapping")
        st.info("Pick the **data columns** you want to associate with ontology terms (e.g., phenotypes, diseases).")

        columns_to_map = st.multiselect(
            "Columns to Map to Ontologies",
            options=all_columns,
            default=[],  # you can choose a default or keep it empty
            help="Select one or more columns to perform ontology mapping."
        )

        # -------------------------------------------------------------------------
        # 3) For each chosen column, show suggestions & let user override
        # -------------------------------------------------------------------------
        st.subheader("B) Review & Edit Ontology Suggestions")

        config = st.session_state['config']
        available_ontologies = config.get('ontologies', {})    # e.g. {'HPO': {...}, 'DO': {...}, ...}
        available_ontology_ids = list(available_ontologies.keys())

        if 'phenotype_columns' not in st.session_state:
            st.session_state['phenotype_columns'] = {}

        col_mappings = {}

        for col in columns_to_map:
            with st.expander(f"Configure Mapping for Column: {col}", expanded=False):
                if col in sample_df.columns:
                    # show some quick stats
                    st.write(f"**Data type**: {sample_df[col].dtype}")
                    missing_count = sample_df[col].isna().sum()
                    st.write(f"**Missing values**: {missing_count} ({missing_count / len(sample_df) * 100:.1f}%)")

                    sample_vals = sample_df[col].dropna().unique()[:5]
                    if len(sample_vals) > 0:
                        st.write("**Sample values**:", ", ".join(map(str, sample_vals)))

                    # Ontology suggestions
                    suggested_onts = suggest_ontologies(col, sample_df[col], available_ontologies)
                    if suggested_onts:
                        st.info(f"**Suggested ontologies** for '{col}': {', '.join(suggested_onts)}")
                    else:
                        st.info("No specific ontology suggestions found for this column.")
                else:
                    # If col not in preview df at all, skip sample stats but user can still map it
                    st.write("Column not present in preview, but you can still map it if it exists in other files.")

                    # We won't do 'suggest_ontologies' because we have no data for that col in sample_df
                    suggested_onts = []

                selected_for_col = st.multiselect(
                    f"Map column '{col}' to these ontologies:",
                    options=available_ontology_ids,
                    default=suggested_onts,
                    format_func=lambda x: f"{x} - {available_ontologies[x]['name']}" if x in available_ontologies else x
                )
                if selected_for_col:
                    col_mappings[col] = selected_for_col

        # save the final column->ontologies mapping in session
        st.session_state['phenotype_columns'] = col_mappings

        # -------------------------------------------------------------------------
        # 4) Select Unique Identifier columns
        # -------------------------------------------------------------------------
        st.subheader("C) Select Unique Identifier Columns")
        st.info("Choose one or more columns that uniquely identify each record (e.g. 'PatientID').")

        chosen_ids = st.multiselect(
            "Unique Identifier Columns",
            options=all_columns,
            default=[],
            help="These columns together should form a unique key for each row."
        )

        if not chosen_ids:
            st.error("You must select at least one column as a unique identifier before proceeding.")
            st.stop()

        st.session_state['unique_identifiers_list'] = chosen_ids

        # -------------------------------------------------------------------------
        # 5) Summary of mappings & Next steps
        # -------------------------------------------------------------------------
        st.subheader("D) Summary of Mappings")
        if st.session_state['phenotype_columns']:
            st.write("**Final Column → Ontologies Mappings**")
            mapping_summary = {
                col: onts for col, onts in st.session_state['phenotype_columns'].items()
            }
            st.json(mapping_summary)
        else:
            st.write("No columns mapped yet.")

        st.write(f"**Unique IDs chosen**: {chosen_ids}")

        # proceed if we have at least one mapping
        if st.session_state['phenotype_columns']:
            st.success("Mapping configuration complete!")
            st.session_state.steps_completed["Select Unique Identifier"] = True
            if st.button("Proceed to Select Ontologies & Impute"):
                proceed_to_step("Select Ontologies & Impute")
        else:
            st.warning("Please map at least one column to continue.")



    ##########################################################
    # Step 4: Imputation Configuration
    ##########################################################
    elif st.session_state.current_step == "Select Ontologies & Impute":
        st.header("Step 4: Select Ontologies & Impute")

        # Make sure we have a union_of_columns
        if "union_of_columns" not in st.session_state or not st.session_state["union_of_columns"]:
            st.error("No columns found. Please go back and upload your data first.")
            st.stop()

        all_columns = st.session_state["union_of_columns"]
        config = st.session_state['config']
        default_strategies = config.get('imputation_strategies', {})
        advanced_methods = config.get('advanced_imputation_methods', [])

        st.subheader("Configure Imputation Strategy")
        global_strategy = st.selectbox(
            "Default Imputation Strategy",
            options=['none', 'mean', 'median', 'mode'] + advanced_methods,
            index=0,
            help="Used for columns without specific overrides"
        )

        st.subheader("Column-Specific Overrides")
        column_strategies = {}

        for col in all_columns:
            with st.expander(f"Column: {col}", expanded=False):
                # We'll guess from config or fallback to global
                suggested = default_strategies.get(col, global_strategy)

                strategy_options = ['Use Default', 'none', 'mean', 'median', 'mode'] + advanced_methods
                default_index = 0
                if suggested in strategy_options:
                    default_index = strategy_options.index(suggested)

                strategy = st.selectbox(
                    f"Imputation strategy for {col}",
                    options=strategy_options,
                    index=default_index,
                    key=f"impute_{col}"
                )
                if strategy != 'Use Default':
                    column_strategies[col] = strategy

        st.session_state['imputation_config'] = {
            'global_strategy': global_strategy,
            'column_strategies': column_strategies
        }

        st.markdown("---")
        st.success("Imputation configuration complete!")
        st.session_state.steps_completed["Select Ontologies & Impute"] = True

        if st.button("Proceed to Run QC and View Results"):
            proceed_to_step("Run QC and View Results")

    ###############################################################################
    # Step 5) Run QC and View Results (merged step) - REPLACE ONLY THIS BLOCK
    ###############################################################################
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

                        # 1) Save schema.json
                        schema_path = os.path.join(tmpdirname, "schema.json")
                        with open(schema_path, 'w') as f:
                            json.dump(st.session_state['schema'], f)

                        # 2) Save config.yaml
                        config_path = os.path.join(tmpdirname, "config.yaml")
                        with open(config_path, 'w') as f:
                            yaml.dump(st.session_state['config'], f)

                        # 3) Save custom mappings if provided
                        if st.session_state['custom_mappings_data']:
                            custom_mappings_path = os.path.join(tmpdirname, "custom_mapping.json")
                            with open(custom_mappings_path, 'w') as f:
                                json.dump(st.session_state['custom_mappings_data'], f)
                        else:
                            custom_mappings_path = None

                        input_paths = []
                        for fname, df_in_memory in st.session_state["multi_dfs"].items():
                            local_path = preserve_original_format_and_save(
                                df_in_memory, 
                                original_filename=fname, 
                                out_dir=tmpdirname
                            )
                            input_paths.append(local_path)                            
                        
                        if not input_paths:
                            st.error("No input files found to process.")
                            st.stop()

                        # 5) Initialize OntologyMapper
                        ontology_mapper = OntologyMapper(config_path)

                        # 6) Grab user’s imputation settings from session
                        impute_config = st.session_state.get('imputation_config', {})
                        impute_strategy_value = impute_config.get('global_strategy', 'none')
                        field_strategies = impute_config.get('column_strategies', {})

                        # 7) Prepare output directory
                        output_dir = os.path.join(tmpdirname, "reports")
                        os.makedirs(output_dir, exist_ok=True)

                        # 8) Clear previous results
                        st.session_state['processing_results'] = []

                        # 9) Process each local CSV with process_file
                        total_files = len(input_paths)
                        progress_bar = st.progress(0)
                        current_progress = 0
                        progress_increment = 100 / total_files if total_files > 0 else 0

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
                                    impute_strategy=impute_strategy_value,
                                    field_strategies=field_strategies,
                                    output_dir=output_dir,
                                    target_ontologies=st.session_state.get('ontologies_selected_list', []),
                                    phenotype_columns=st.session_state.get('phenotype_columns')
                                )
                                # Store the result
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

        # Once processing is done, show results in tabs
        if st.session_state.steps_completed.get("Run QC and View Results", False):
            st.header("Results")
            if 'processing_results' in st.session_state and st.session_state['processing_results']:
                tab_labels = [os.path.basename(fname) for fname, _, _ in st.session_state['processing_results']]
                tabs = st.tabs(tab_labels)

                for (file_name, result_dict, output_dir), tab in zip(st.session_state['processing_results'], tabs):
                    with tab:
                        st.subheader(f"Results for {file_name}")
                        file_status = result_dict['status']
                        if file_status == 'Processed':
                            st.success("File processed successfully.")
                        elif file_status == 'ProcessedWithWarnings':
                            st.warning("File processed with warnings or schema violations.")
                        elif file_status == 'Invalid':
                            st.warning(f"File failed validation: {result_dict['error']}")
                        else:
                            st.error(f"File encountered an error: {result_dict['error']}")

                        processed_data_path = result_dict.get('processed_file_path')
                        if not processed_data_path or not os.path.exists(processed_data_path):
                            st.error("Processed data file not found. No partial output available.")
                            continue

                        # ---------------------------------------------------------
                        # Read the processed CSV
                        # ---------------------------------------------------------
                        try:
                            df = pd.read_csv(processed_data_path)
                        except Exception as ex:
                            st.error(f"Failed to read processed data: {str(ex)}")
                            continue

                        st.write("### Sample of Processed Data:")
                        st.dataframe(df.head(5))

                        # Build summary
                        validation_res = result_dict.get('validation_results', {})
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

                        # ----------------------------------------------------------------
                        # Display the invalid cells (highlighting), but remove re-validate
                        # ----------------------------------------------------------------
                        invalid_mask = validation_res.get("Invalid Mask", pd.DataFrame())
                        if invalid_mask.empty or not invalid_mask.any().any():
                            st.write("No invalid cells found or no mask returned.")
                        else:
                            st.write("### Highlighting Invalid Cells (read-only)")
                            
                            key_prefix = file_name.replace('.', '_')

                            # Keep an in-memory editable copy, but no re-validation
                            if f"{key_prefix}_df" not in st.session_state:
                                st.session_state[f"{key_prefix}_df"] = df.copy()
                            if f"{key_prefix}_mask" not in st.session_state:
                                st.session_state[f"{key_prefix}_mask"] = invalid_mask.copy()

                            editable_df = display_editable_grid_with_highlighting(
                                st.session_state[f"{key_prefix}_df"].copy(),
                                st.session_state[f"{key_prefix}_mask"].copy(),
                                allow_edit=True
                            )

                            st.write("Edits here do NOT trigger re-validation; this is just for reference.")

                            # Optional CSV download of invalid highlights
                            st.write("#### Download Invalid-Cell Highlights")
                            merged_df = df.copy()
                            invalid_cols = invalid_mask.columns.intersection(df.columns)
                            for col in invalid_cols:
                                newcol = f"{col}_isInvalid"
                                merged_df[newcol] = invalid_mask[col].astype(bool)

                            csv_data = merged_df.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download CSV with Invalid Highlights",
                                data=csv_data,
                                file_name=f"{os.path.splitext(file_name)[0]}_invalid_highlights.csv",
                                mime='text/csv'
                            )

                        # ================
                        # Visual Summaries
                        # ================
                        st.write("### Visual Summaries")
                        figs = create_visual_summary(
                            df=df,
                            phenotype_columns=st.session_state.get('phenotype_columns'),
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
                        q_scores = result_dict.get('quality_scores', {})
                        for score_name, score_val in q_scores.items():
                            st.write(f"- **{score_name}**: {score_val:.2f}%")

                        st.write("### Downloads")
                        report_buffer = io.BytesIO()
                        generate_qc_report(
                            validation_results=validation_res,
                            missing_data=result_dict.get('missing_data', pd.Series()),
                            flagged_records_count=result_dict.get('flagged_records_count', 0),
                            mapping_success_rates=result_dict.get('mapping_success_rates', {}),
                            visualization_images=result_dict.get('visualization_images', []),
                            impute_strategy=impute_strategy_value,
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