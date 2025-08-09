Installation
==========================

Requirements
----------------------------------------------------

PhenoQC requires Python 3.9 or later. The package and its dependencies can be installed using pip.

Using pip
---------

The easiest way to install PhenoQC is using pip:

.. code-block:: bash

    pip install phenoqc

From Source
----------

To install PhenoQC from source:

.. code-block:: bash

    git clone https://github.com/jorgeMFS/PhenoQC.git
    cd PhenoQC
    pip install -e .

Development without install
---------------------------

When running directly from the source tree during development, prefer invoking as a module and ensuring the ``src/`` directory is on the Python path:

.. code-block:: bash

    # In the project root
    export PYTHONPATH=src:$PYTHONPATH
    python -m phenoqc --help
    python -m phenoqc.cli  # alternative explicit module

GUI
---

Launch the Streamlit-based GUI:

.. code-block:: bash

    python run_gui.py

Dependencies
-----------

PhenoQC requires the following Python packages:

- pandas
- jsonschema
- requests
- plotly
- reportlab
- streamlit
- pyyaml
- watchdog
- kaleido
- tqdm
- Pillow
- scikit-learn
- fancyimpute
- fastjsonschema
- pronto
- rapidfuzz

These dependencies will be automatically installed when installing PhenoQC using pip. 