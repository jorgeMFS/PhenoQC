import zipfile
import tempfile

def extract_zip(zip_path, extract_to=None):
    """
    Extracts a ZIP archive to 'extract_to'. If 'extract_to' is None, uses a temp dir.
    Returns (extraction_dir, error_msg or None).
    """
    if extract_to is None:
        extract_to = tempfile.mkdtemp(prefix="phenoqc_zip_")

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            # Filter out macOS-specific files and directories, etc.
            members = [
                f for f in zip_file.namelist()
                if not f.startswith('__MACOSX/')
                and not f.startswith('._')
                and not f.endswith('.DS_Store')
            ]
            zip_file.extractall(path=extract_to, members=members)
        return extract_to, None
    except zipfile.BadZipFile:
        return None, f"The file '{zip_path}' is not a valid ZIP archive."
    except Exception as e:
        return None, f"An error occurred during ZIP extraction of '{zip_path}': {str(e)}"
