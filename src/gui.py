import streamlit as st
from batch_processing import batch_process
import os
import tempfile
import zipfile

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
    st.title("PhenoQC - Phenotypic Data Quality Control Toolkit")
    
    st.sidebar.header("Configuration")
    schema_file = st.sidebar.file_uploader("Upload JSON Schema", type=["json"])
    mapping_file = st.sidebar.file_uploader("Upload HPO Mapping", type=["json"])
    custom_mapping_file = st.sidebar.file_uploader("Upload Custom Mapping (Optional)", type=["json"])
    impute_strategy = st.sidebar.selectbox("Imputation Strategy", options=['mean', 'median'])
    
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
        if not schema_file or not mapping_file:
            st.error("Please upload both the schema and mapping files.")
            return
        
        if data_source_option == 'Upload Files' and not uploaded_files:
            st.error("Please upload at least one phenotypic data file.")
            return
        elif data_source_option == 'Upload Directory (ZIP)' and not uploaded_zip:
            st.error("Please upload a ZIP archive containing your data files.")
            return
        
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
                
                # Collect supported files recursively if enabled
                supported_extensions = {'.csv', '.tsv', '.json'}
                if enable_recursive:
                    collected_files = collect_supported_files(extract_dir, supported_extensions)
                    input_paths.extend(collected_files)
                else:
                    # Non-recursive: only scan the top-level directory
                    for file in os.listdir(extract_dir):
                        file_path = os.path.join(extract_dir, file)
                        if os.path.isfile(file_path):
                            ext = os.path.splitext(file_path)[1].lower()
                            if ext in supported_extensions:
                                input_paths.append(file_path)
                
                if not input_paths:
                    st.error("No supported data files found in the uploaded archive.")
                    return
            
            # Save schema and mapping files
            schema_path = os.path.join(tmpdirname, "schema.json")
            with open(schema_path, 'wb') as f:
                f.write(schema_file.getbuffer())
            
            mapping_path = os.path.join(tmpdirname, "hpo_mapping.json")
            with open(mapping_path, 'wb') as f:
                f.write(mapping_file.getbuffer())
            
            custom_mapping_path = None
            if custom_mapping_file:
                custom_mapping_path = os.path.join(tmpdirname, "custom_mapping.json")
                with open(custom_mapping_path, 'wb') as f:
                    f.write(custom_mapping_file.getbuffer())
            
            # Define output directory
            output_dir = os.path.join(tmpdirname, "reports")
            os.makedirs(output_dir, exist_ok=True)
            
            # Run batch processing
            with st.spinner("Processing files..."):
                results = batch_process(
                    files=input_paths,
                    schema_path=schema_path,
                    hpo_terms_path=mapping_path,
                    custom_mappings_path=custom_mapping_path,
                    impute_strategy=impute_strategy,
                    output_dir=output_dir
                )
            
            # Display results
            st.header("Processing Results")
            for result in results:
                file = os.path.basename(result['file'])
                if result['status'] == 'Processed':
                    st.success(f"{file} processed successfully.")
                    report_path = os.path.join(output_dir, os.path.splitext(file)[0] + "_report.pdf")
                    if os.path.exists(report_path):
                        with open(report_path, 'rb') as f:
                            st.download_button(
                                label=f"Download Report for {file}",
                                data=f,
                                file_name=os.path.basename(report_path),
                                mime='application/pdf'
                            )
                elif result['status'] == 'Invalid':
                    st.warning(f"{file} failed validation: {result['error']}")
                else:
                    st.error(f"{file} encountered an error: {result['error']}")

if __name__ == '__main__':
    main()