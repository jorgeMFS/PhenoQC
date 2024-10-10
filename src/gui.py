import streamlit as st
from batch_processing import batch_process
import os

def main():
    st.title("PhenoQC - Phenotypic Data Quality Control Toolkit")
    
    st.sidebar.header("Configuration")
    schema_file = st.sidebar.file_uploader("Upload JSON Schema", type=["json"])
    mapping_file = st.sidebar.file_uploader("Upload HPO Mapping", type=["json"])
    custom_mapping_file = st.sidebar.file_uploader("Upload Custom Mapping (Optional)", type=["json"])
    impute_strategy = st.sidebar.selectbox("Imputation Strategy", options=['mean', 'median'])
    
    uploaded_files = st.file_uploader("Upload Phenotypic Data Files", type=["csv", "tsv", "json"], accept_multiple_files=True)
    
    if st.button("Run Quality Control"):
        if not schema_file or not mapping_file or not uploaded_files:
            st.error("Please upload the schema, mapping files and at least one data file.")
            return
        
        # Save uploaded files to a temporary directory
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdirname:
            input_paths = []
            for uploaded_file in uploaded_files:
                file_path = os.path.join(tmpdirname, uploaded_file.name)
                with open(file_path, 'wb') as f:
                    f.write(uploaded_file.getbuffer())
                input_paths.append(file_path)
            
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
            
            # Determine file type
            file_types = set()
            for f in uploaded_files:
                if f.name.endswith('.csv'):
                    file_types.add('csv')
                elif f.name.endswith('.tsv'):
                    file_types.add('tsv')
                elif f.name.endswith('.json'):
                    file_types.add('json')
            if len(file_types) > 1:
                st.error("Please upload files of the same type.")
                return
            file_type = file_types.pop()
            
            # Run batch processing
            results = batch_process(
                files=input_paths,
                file_type=file_type,
                schema_path=schema_path,
                hpo_terms_path=mapping_path,
                custom_mappings_path=custom_mapping_path,
                impute_strategy=impute_strategy
            )
            
            # Display results
            for result in results:
                file = os.path.basename(result['file'])
                if result['status'] == 'Processed':
                    st.success(f"{file} processed successfully.")
                    report_path = os.path.join(output_dir, os.path.splitext(file)[0] + "_report.pdf")
                    if os.path.exists(report_path):
                        with open(report_path, 'rb') as f:
                            st.download_button(label=f"Download Report for {file}", data=f, file_name=os.path.basename(report_path), mime='application/pdf')
                elif result['status'] == 'Invalid':
                    st.warning(f"{file} failed validation: {result['error']}")
                else:
                    st.error(f"{file} encountered an error: {result['error']}")

if __name__ == '__main__':
    main()