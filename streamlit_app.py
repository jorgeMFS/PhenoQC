#!/usr/bin/env python3

"""
Streamlit entrypoint for Streamlit Community Cloud.

This module imports and runs the PhenoQC GUI without spawning a subprocess.
It adjusts sys.path so that `src/` is importable when deployed.
"""

import os
import sys


def main() -> None:
    # Ensure `src/` is importable when running on Streamlit Cloud
    project_root = os.path.dirname(os.path.abspath(__file__))
    src_path = os.path.join(project_root, "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    # Import and run the GUI main
    from phenoqc.gui import main as gui_main

    gui_main()


if __name__ == "__main__":
    main()


