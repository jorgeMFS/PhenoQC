#!/usr/bin/env python3
import os
import sys
import subprocess

if __name__ == '__main__':
    # Get the project root directory
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # Set PYTHONPATH to include the project root
    os.environ['PYTHONPATH'] = project_root
    
    # Import and run the main function
    from src.gui import main
    
    # Create a temporary file with the main function call
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write('from src.gui import main\nif __name__ == "__main__":\n    main()')
        temp_path = f.name
    
    try:
        # Run streamlit using the temporary file
        subprocess.run(["streamlit", "run", temp_path])
    finally:
        # Clean up the temporary file
        os.unlink(temp_path) 