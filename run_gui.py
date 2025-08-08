#!/usr/bin/env python3
import os
import sys
import subprocess

if __name__ == '__main__':
    # Get the project root directory
    project_root = os.path.dirname(os.path.abspath(__file__))

    # Add the 'src' directory to PYTHONPATH and sys.path for imports
    src_path = os.path.join(project_root, 'src')
    os.environ['PYTHONPATH'] = src_path
    sys.path.insert(0, src_path)

    # Import and run the main function
    from phenoqc.gui import main

    # Create a temporary file with the main function call
    import tempfile

    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write('from phenoqc.gui import main\nif __name__ == "__main__":\n    main()')
        temp_path = f.name

    try:
        # Run streamlit using the temporary file
        subprocess.run(["streamlit", "run", temp_path])
    finally:
        # Clean up the temporary file
        os.unlink(temp_path)
