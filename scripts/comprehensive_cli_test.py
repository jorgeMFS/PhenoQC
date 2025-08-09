#!/usr/bin/env python3
"""
Compatibility wrapper for historical name: comprehensive_cli_test.py

Delegates to the standardized script `e2e_medium_cli_test.py` to keep
older docs and references functioning.
"""

import os
import runpy

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(CURRENT_DIR, "e2e_medium_cli_test.py")

if __name__ == "__main__":
    runpy.run_path(TARGET, run_name="__main__")


