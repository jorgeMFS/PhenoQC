"""Entry point for running PhenoQC as a module.

This module simply forwards execution to the CLI ``main`` function so that
``python -m phenoqc`` behaves the same as invoking the ``phenoqc`` command
directly.
"""

from phenoqc.cli import main


if __name__ == "__main__":
    main()

