import unittest
import json
import tempfile
import os
import yaml
from src.mapping import OntologyMapper

class TestOntologyMapper(unittest.TestCase):
    def setUp(self):
        # Sample ontology data
        self.hpo_terms = [
            {"id": "HP:0000822", "name": "Hypertension", "synonyms": ["High blood pressure"]},
            {"id": "HP:0001627", "name": "Diabetes", "synonyms": ["Sugar diabetes"]},
            {"id": "HP:0002090", "name": "Asthma", "synonyms": ["Reactive airway disease"]},
            {"id": "HP:0001511", "name": "Obesity", "synonyms": ["Fatty syndrome"]},
            {"id": "HP:0004322", "name": "Anemia", "synonyms": ["Lack of red blood cells"]}
        ]

        self.do_terms = [
            {"id": "DOID:0050167", "name": "Hypertension", "synonyms": ["High blood pressure"]},
            {"id": "DOID:1612", "name": "Diabetes Mellitus", "synonyms": ["Sugar diabetes", "Diabetes"]},  # Added 'Diabetes'
            {"id": "DOID:9352", "name": "Asthma", "synonyms": ["Reactive airway disease"]},
            {"id": "DOID:9351", "name": "Obesity", "synonyms": ["Fatty syndrome"]},
            {"id": "DOID:1388", "name": "Anemia", "synonyms": ["Lack of red blood cells"]}
        ]

        # Create temporary ontology JSON files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.hpo_file = os.path.join(self.temp_dir.name, "HPO.json")
        self.do_file = os.path.join(self.temp_dir.name, "DO.json")
        with open(self.hpo_file, 'w') as f:
            json.dump(self.hpo_terms, f)
        with open(self.do_file, 'w') as f:
            json.dump(self.do_terms, f)

        # Create a temporary config.yaml
        self.config_file = os.path.join(self.temp_dir.name, "config.yaml")
        yaml_dump = """
ontologies:
  HPO:
    name: Human Phenotype Ontology
    file: {}
  DO:
    name: Disease Ontology
    file: {}
default_ontology: HPO
""".format(self.hpo_file, self.do_file)
        with open(self.config_file, 'w') as f:
            f.write(yaml_dump)

        # Create a temporary custom mapping file
        self.custom_mapping_file = os.path.join(self.temp_dir.name, "custom_mapping.json")
        custom_mappings = {
            "Obesity": "HP:0001511",  # Assuming mapping to HPO
            "Anemia": "DOID:1388"      # Assuming mapping to DO
        }
        with open(self.custom_mapping_file, 'w') as f:
            json.dump(custom_mappings, f)

        # Initialize OntologyMapper
        self.mapper = OntologyMapper(config_path=self.config_file)

    def tearDown(self):
        # Clean up temporary directory and files
        self.temp_dir.cleanup()

    def test_initialization(self):
        # Test if OntologyMapper initializes correctly
        supported = self.mapper.get_supported_ontologies()
        self.assertIn("HPO", supported)
        self.assertIn("DO", supported)
        self.assertEqual(self.mapper.default_ontology, "HPO")

    def test_map_terms_default_ontology(self):
        # Test mapping terms using the default ontology (HPO)
        terms = ["Hypertension", "Asthma", "Unknown Term"]
        mappings = self.mapper.map_terms(terms)
        expected = {
            "Hypertension": {"HPO": "HP:0000822"},
            "Asthma": {"HPO": "HP:0002090"},
            "Unknown Term": {"HPO": None}
        }
        self.assertEqual(mappings, expected)

    def test_map_terms_multiple_ontologies(self):
        # Test mapping terms across multiple ontologies (HPO and DO)
        terms = ["Hypertension", "Diabetes", "Obesity", "Anemia", "Unknown Term"]
        ontologies = ["HPO", "DO"]
        mappings = self.mapper.map_terms(terms, target_ontologies=ontologies)
        expected = {
            "Hypertension": {"HPO": "HP:0000822", "DO": "DOID:0050167"},
            "Diabetes": {"HPO": "HP:0001627", "DO": "DOID:1612"},
            "Obesity": {"HPO": "HP:0001511", "DO": "DOID:9351"},
            "Anemia": {"HPO": "HP:0004322", "DO": "DOID:1388"},
            "Unknown Term": {"HPO": None, "DO": None}
        }
        self.assertEqual(mappings, expected)

    def test_map_terms_with_custom_mappings(self):
        # Test mapping terms with custom mappings applied
        terms = ["Obesity", "Anemia", "Hypertension"]
        ontologies = ["HPO", "DO"]
        custom_mappings = {}
        with open(self.custom_mapping_file, 'r') as f:
            custom_mappings = json.load(f)
        mappings = self.mapper.map_terms(terms, target_ontologies=ontologies, custom_mappings=custom_mappings)
        expected = {
            "Obesity": {"HPO": "HP:0001511", "DO": "DOID:9351"},  # Custom mapping to HPO remains the same
            "Anemia": {"HPO": "HP:0004322", "DO": "DOID:1388"},  # Custom mapping to DO
            "Hypertension": {"HPO": "HP:0000822", "DO": "DOID:0050167"}
        }
        self.assertEqual(mappings, expected)

    def test_map_terms_with_synonyms(self):
        # Test mapping terms using synonyms
        terms = ["High blood pressure", "Sugar diabetes", "Reactive airway disease"]
        ontologies = ["HPO", "DO"]
        mappings = self.mapper.map_terms(terms, target_ontologies=ontologies)
        expected = {
            "High blood pressure": {"HPO": "HP:0000822", "DO": "DOID:0050167"},
            "Sugar diabetes": {"HPO": "HP:0001627", "DO": "DOID:1612"},
            "Reactive airway disease": {"HPO": "HP:0002090", "DO": "DOID:9352"}
        }
        self.assertEqual(mappings, expected)

    def test_get_supported_ontologies(self):
        # Test retrieval of supported ontologies
        supported = self.mapper.get_supported_ontologies()
        self.assertListEqual(supported, ["HPO", "DO"])

    def test_add_new_ontology(self):
        # Create sample MPO data
        mpo_terms = [
            {"id": "MP:0001943", "name": "Obesity", "synonyms": []},
            {"id": "MP:0001902", "name": "Abnormal behavior", "synonyms": []}
        ]

        # Create temporary MPO ontology file
        mpo_file = os.path.join(self.temp_dir.name, "MPO.json")
        with open(mpo_file, 'w') as f:
            json.dump(mpo_terms, f)

        # Load existing config
        with open(self.config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Add new ontology
        new_ontology = {
            "name": "Mammalian Phenotype Ontology",
            "file": mpo_file
        }
        config_data['ontologies']['MPO'] = new_ontology  # Add under 'ontologies'

        # Write updated config back to file
        with open(self.config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        # Reload OntologyMapper
        self.mapper = OntologyMapper(config_path=self.config_file)
        
        supported = self.mapper.get_supported_ontologies()
        self.assertIn("MPO", supported)
        
        # Test mapping with the new ontology
        terms = ["Obesity", "Abnormal behavior"]
        ontologies = ["MPO"]
        mappings = self.mapper.map_terms(terms, target_ontologies=ontologies)
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
            OntologyMapper(config_path=invalid_config_path)

    def test_missing_ontology_file(self):
        # Test initialization with a missing ontology file
        missing_ontology_path = os.path.join(self.temp_dir.name, "MissingOntology.json")
        config_data = {
            "ontologies": {
                "HPO": {
                    "name": "Human Phenotype Ontology",
                    "file": "NonExistentFile.json"
                }
            },
            "default_ontology": "HPO"
        }
        missing_config_file = os.path.join(self.temp_dir.name, "missing_config.yaml")
        with open(missing_config_file, 'w') as f:
            yaml_dump = """
ontologies:
  HPO:
    name: Human Phenotype Ontology
    file: {}
default_ontology: HPO
""".format("NonExistentFile.json")
            f.write(yaml_dump)
        
        with self.assertRaises(FileNotFoundError):
            OntologyMapper(config_path=missing_config_file)

if __name__ == '__main__':
    unittest.main()