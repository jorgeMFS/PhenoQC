import unittest
import json
import tempfile
import os
import yaml
from src.mapping import OntologyMapper

class TestOntologyMapper(unittest.TestCase):
    def setUp(self):
        # Suppress ResourceWarnings temporarily
        import warnings
        warnings.filterwarnings("ignore", category=ResourceWarning)

        # Create a temporary directory for ontology files
        self.temp_dir = tempfile.TemporaryDirectory()

        # Sample HPO data in OBO format
        self.hpo_terms = """
format-version: 1.2
data-version: releases/2021-02-01
ontology: Human Phenotype Ontology

[Term]
id: HP:0000822
name: Hypertension
synonym: "High blood pressure" EXACT []

[Term]
id: HP:0001627
name: Diabetes
synonym: "Sugar diabetes" EXACT []

[Term]
id: HP:0002090
name: Asthma
synonym: "Reactive airway disease" EXACT []
"""

        # Sample DO data in OBO format
        self.do_terms = """
format-version: 1.2
data-version: releases/2021-02-01
ontology: Disease Ontology

[Term]
id: DOID:0050167
name: Hypertension
synonym: "High blood pressure" EXACT []

[Term]
id: DOID:1612
name: Diabetes Mellitus
synonym: "Sugar diabetes" EXACT []
synonym: "Diabetes" EXACT []

[Term]
id: DOID:9352
name: Asthma
synonym: "Reactive airway disease" EXACT []

[Term]
id: DOID:9351
name: Obesity
synonym: "Fatty syndrome" EXACT []

[Term]
id: DOID:1388
name: Anemia
synonym: "Lack of red blood cells" EXACT []
"""

        # Write HPO and DO ontology files
        self.hpo_file = os.path.join(self.temp_dir.name, "HPO.obo")
        with open(self.hpo_file, 'w') as f:
            f.write(self.hpo_terms)

        self.do_file = os.path.join(self.temp_dir.name, "DO.obo")
        with open(self.do_file, 'w') as f:
            f.write(self.do_terms)

        # Create a temporary configuration file
        self.config_file = os.path.join(self.temp_dir.name, 'config.yaml')
        with open(self.config_file, 'w') as f:
            yaml.dump({
                'ontologies': {
                    'HPO': {
                        'name': 'Human Phenotype Ontology',
                        'file': self.hpo_file
                    },
                    'DO': {
                        'name': 'Disease Ontology',
                        'file': self.do_file
                    }
                },
                'default_ontologies': ['HPO', 'DO']
            }, f)

        # Initialize OntologyMapper with the path as a single argument
        self.mapper = OntologyMapper(self.config_file)

    def tearDown(self):
        # Clean up temporary directories and files
        self.temp_dir.cleanup()

    def test_initialization(self):
        # Test if OntologyMapper initializes correctly
        supported = self.mapper.get_supported_ontologies()
        self.assertIn("HPO", supported)
        self.assertIn("DO", supported)
        self.assertEqual(sorted(self.mapper.default_ontologies), sorted(["HPO", "DO"]))

    def test_get_supported_ontologies(self):
        supported = self.mapper.get_supported_ontologies()
        self.assertListEqual(sorted(supported), sorted(["HPO", "DO"]))

    def test_map_terms_default_ontology(self):
        # Define terms to map
        terms = ["Hypertension", "Asthma", "Unknown Term"]
        # Perform mapping using default ontologies (HPO and DO)
        mappings = self.mapper.map_terms(terms)
        # Define expected mappings
        expected = {
            "Hypertension": {
                "HPO": "HP:0000822",
                "DO": "DOID:0050167"
            },
            "Asthma": {
                "HPO": "HP:0002090",
                "DO": "DOID:9352"
            },
            "Unknown Term": {
                "HPO": None,
                "DO": None
            }
        }
        self.assertEqual(mappings, expected, "Default term mappings do not match expected values.")

    def test_map_terms_with_synonyms(self):
        # Define terms with synonyms
        terms = ["High blood pressure", "Sugar diabetes", "Reactive airway disease"]
        # Perform mapping using default ontologies (HPO and DO)
        mappings = self.mapper.map_terms(terms)
        # Define expected mappings
        expected = {
            "High blood pressure": {
                "HPO": "HP:0000822",
                "DO": "DOID:0050167"
            },
            "Sugar diabetes": {
                "HPO": "HP:0001627",
                "DO": "DOID:1612"
            },
            "Reactive airway disease": {
                "HPO": "HP:0002090",
                "DO": "DOID:9352"
            }
        }
        self.assertEqual(mappings, expected, "Synonym term mappings do not match expected values.")

    def test_map_terms_with_custom_mappings(self):
        # Add Mammalian Phenotype Ontology (MPO)
        mpo_terms = """
format-version: 1.2
data-version: releases/2021-02-01
ontology: Mammalian Phenotype Ontology

[Term]
id: MP:0001943
name: Obesity
synonym: "Fatty syndrome" EXACT []

[Term]
id: MP:0001902
name: Abnormal behavior
synonym: "Behaviors differing from the norm" EXACT []
"""
        # Create temporary MPO ontology file
        mpo_file = os.path.join(self.temp_dir.name, "MPO.obo")
        with open(mpo_file, 'w') as f:
            f.write(mpo_terms)

        # Update the configuration to include MPO
        with open(self.config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        config_data['ontologies']['MPO'] = {
            'name': 'Mammalian Phenotype Ontology',
            'file': mpo_file
        }
        config_data['default_ontologies'].append('MPO')

        with open(self.config_file, 'w') as f:
            yaml.dump(config_data, f)

        # Reload OntologyMapper with updated config
        self.mapper = OntologyMapper(self.config_file)

        # Verify that MPO is now supported
        supported = self.mapper.get_supported_ontologies()
        self.assertIn("MPO", supported)

        # Define terms to map, including ones from MPO
        terms = ["Obesity", "Abnormal behavior"]
        # Perform mapping using all default ontologies (HPO, DO, MPO)
        mappings = self.mapper.map_terms(terms)
        # Define expected mappings
        expected = {
            "Obesity": {
                "HPO": None,
                "DO": "DOID:9351",
                "MPO": "MP:0001943"
            },
            "Abnormal behavior": {
                "HPO": None,
                "DO": None,
                "MPO": "MP:0001902"
            }
        }
        self.assertEqual(mappings, expected, "Custom term mappings do not match expected values.")

    def test_add_new_ontology(self):
        # Create sample MPO data in OBO format
        mpo_terms = """
format-version: 1.2
data-version: releases/2021-02-01
ontology: Mammalian Phenotype Ontology

[Term]
id: MP:0001943
name: Obesity
synonym: "Fatty syndrome" EXACT []

[Term]
id: MP:0001902
name: Abnormal behavior
synonym: "Behaviors differing from the norm" EXACT []
"""
        # Create temporary MPO ontology file
        mpo_file = os.path.join(self.temp_dir.name, "MPO.obo")
        with open(mpo_file, 'w') as f:
            f.write(mpo_terms)

        # Load existing config
        with open(self.config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Add new ontology
        config_data['ontologies']['MPO'] = {
            'name': 'Mammalian Phenotype Ontology',
            'file': mpo_file
        }
        config_data['default_ontologies'].append('MPO')

        with open(self.config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        # Reload OntologyMapper
        self.mapper = OntologyMapper(self.config_file)
        
        # Verify that MPO is now supported
        supported = self.mapper.get_supported_ontologies()
        self.assertIn("MPO", supported)
        
        # Define terms to map
        terms = ["Obesity", "Abnormal behavior"]
        # Perform mapping using MPO only
        mappings = self.mapper.map_terms(terms, target_ontologies=["MPO"])
        
        # Define expected mappings
        expected = {
            "Obesity": {"MPO": "MP:0001943"},
            "Abnormal behavior": {"MPO": "MP:0001902"}
        }
        self.assertEqual(mappings, expected)

    def test_invalid_config_file(self):
        # Test initialization with an invalid config file
        invalid_config_path = os.path.join(self.temp_dir.name, "invalid_config.yaml")
        with open(invalid_config_path, 'w') as f:
            f.write("invalid_yaml: [unbalanced brackets")

        with self.assertRaises(Exception):
            OntologyMapper(invalid_config_path)

    def test_missing_ontology_file(self):
        # Test initialization with a missing ontology file
        missing_ontology_path = os.path.join(self.temp_dir.name, "MissingOntology.obo")
        config_data = {
            "ontologies": {
                "HPO": {
                    "name": "Human Phenotype Ontology",
                    "file": "NonExistentFile.obo"
                }
            },
            "default_ontologies": ["HPO"]
        }
        missing_config_file = os.path.join(self.temp_dir.name, "missing_config.yaml")
        with open(missing_config_file, 'w') as f:
            yaml_dump = f"""
ontologies:
  HPO:
    name: Human Phenotype Ontology
    file: {missing_ontology_path}
default_ontologies: [HPO]
"""
            f.write(yaml_dump)
        
        with self.assertRaises(FileNotFoundError):
            OntologyMapper(missing_config_file)

if __name__ == '__main__':
    unittest.main()
