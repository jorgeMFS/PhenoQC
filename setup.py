from setuptools import setup, find_packages

setup(
    name='PhenoQC',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pandas',
        'jsonschema',
        'requests',
        'plotly',
        'reportlab',
        'streamlit',
        'pyyaml',
        'watchdog',
        'kaleido>=0.1.0',
        'tqdm',
        'Pillow',
        'scikit-learn',
        'fancyimpute',
    ],
    extras_require={
        'test': [
            'pytest',
            'unittest',
        ],
    },
    entry_points={
        'console_scripts': [
            'phenoqc=cli:main',
        ],
    },
    package_data={
        'phenoqc': ['*.json', '*.yaml'],
    },
    include_package_data=True,
    author='Jorge Miguel Ferreira da Silva',
    description='Phenotypic Data Quality Control Toolkit for Genomic Data Infrastructure (GDI)',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/jorgeMFS/PhenoQC',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
    python_requires='>=3.6',
)
