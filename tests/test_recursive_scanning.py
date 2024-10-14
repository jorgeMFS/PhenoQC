import os
import tempfile
import shutil
import subprocess
import json  # Ensure the json module is imported

def create_sample_files(base_dir):
    """
    Creates a nested directory structure with sample data files.
    
    Args:
        base_dir (str): Path to the base temporary directory.
    """
    os.makedirs(os.path.join(base_dir, "sub_dir1"), exist_ok=True)
    os.makedirs(os.path.join(base_dir, "sub_dir2", "nested_dir"), exist_ok=True)

    # Sample CSV file in base directory
    with open(os.path.join(base_dir, "sample_data1.csv"), "w") as f:
        f.write("SampleID,Age,Gender,Phenotype,Measurement\n")
        f.write("S001,34,Male,Hypertension,120\n")

    # Sample TSV file in sub_dir1
    with open(os.path.join(base_dir, "sub_dir1", "sample_data2.tsv"), "w") as f:
        f.write("SampleID\tAge\tGender\tPhenotype\tMeasurement\n")
        f.write("S002\t28\tFemale\tDiabetes\t85\n")

    # Sample JSON file in sub_dir2/nested_dir
    with open(os.path.join(base_dir, "sub_dir2", "nested_dir", "sample_data3.json"), "w") as f:
        json_content = [
            {"SampleID": "S003", "Age": 45, "Gender": "Other", "Phenotype": "Asthma", "Measurement": 95},
            {"SampleID": "S004", "Age": 30, "Gender": "Male", "Phenotype": "Hypertension", "Measurement": None}
        ]
        json.dump(json_content, f, indent=4)  # Proper JSON serialization

def run_phenoqc(input_dir, output_dir, schema_path, mapping_path, impute_strategy):
    """
    Executes the PhenoQC CLI with the specified parameters.
    
    Args:
        input_dir (str): Path to the input directory.
        output_dir (str): Path to the output reports directory.
        schema_path (str): Path to the JSON schema file.
        mapping_path (str): Path to the HPO mapping JSON file.
        impute_strategy (str): Imputation strategy ('mean' or 'median').
    
    Returns:
        subprocess.CompletedProcess: The result of the subprocess execution.
    """
    command = [
        "phenoqc",
        "--input", input_dir,
        "--output", output_dir,
        "--schema", schema_path,
        "--mapping", mapping_path,
        "--impute", impute_strategy,
        "--recursive"
    ]
    print(f"Running command: {' '.join(command)}")
    return subprocess.run(command, capture_output=True, text=True)

def main():
    # Paths to your existing schema and mapping files
    schema_path = "schemas/pheno_schema.json"
    mapping_path = "examples/sample_mapping.json"

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory at {temp_dir}")

        # Create sample files within the temporary directory
        create_sample_files(temp_dir)
        print("Sample files created.")

        # Define the output directory within the temporary directory
        output_dir = os.path.join(temp_dir, "reports")
        
        # Run PhenoQC with recursive scanning enabled
        result = run_phenoqc(
            input_dir=temp_dir,
            output_dir=output_dir,
            schema_path=schema_path,
            mapping_path=mapping_path,
            impute_strategy="median"
        )

        # Output the results
        print("\n--- PhenoQC Output ---")
        print(result.stdout)
        print(result.stderr)
        
        # Verify that reports are generated
        if os.path.exists(output_dir):
            generated_reports = os.listdir(output_dir)
            print(f"\nGenerated reports in '{output_dir}':")
            for report in generated_reports:
                print(f"- {report}")
        else:
            print("❌ Output directory was not created.")

if __name__ == "__main__":
    main()