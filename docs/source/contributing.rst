Contributing
==========================

We welcome contributions to PhenoQC! This document provides guidelines for contributing to the project.

Setting Up Development Environment
----------------------------------------------------

1. Fork the repository on GitHub
2. Clone your fork locally:

   .. code-block:: bash

       git clone https://github.com/YOUR_USERNAME/PhenoQC.git
       cd PhenoQC

3. Create a virtual environment and install dependencies:

   .. code-block:: bash

       python -m venv venv
       source venv/bin/activate  # On Windows: venv\Scripts\activate
       pip install -e ".[dev]"

4. Install pre-commit hooks:

   .. code-block:: bash

       pre-commit install

Making Changes
------------

1. Create a new branch for your changes:

   .. code-block:: bash

       git checkout -b feature-name

2. Make your changes
3. Add tests for any new functionality
4. Run the test suite:

   .. code-block:: bash

       pytest

5. Format your code:

   .. code-block:: bash

       black .

6. Commit your changes:

   .. code-block:: bash

       git add .
       git commit -m "Description of changes"

7. Push to your fork:

   .. code-block:: bash

       git push origin feature-name

8. Submit a Pull Request

Pull Request Guidelines
--------------------

1. Include tests for any new functionality
2. Update documentation as needed
3. Follow the existing code style
4. Include a clear description of the changes
5. Link any related issues

Code Style
---------

We use Black for code formatting and follow PEP 8 guidelines.

Running Tests
-----------

Run the full test suite:

.. code-block:: bash

    pytest

Run with coverage:

.. code-block:: bash

    pytest --cov=src tests/

Building Documentation
-------------------

Build the documentation locally:

.. code-block:: bash

    cd docs
    make html

The built documentation will be in ``docs/build/html``. 