import unittest
from src.mapping import fetch_hpo_terms, map_to_hpo, load_custom_mappings

class TestMappingModule(unittest.TestCase):
    def setUp(self):
        # Assuming 'examples/sample_mapping.json' exists
        self.hpo_terms = [
            {"id": "HP:0000822", "name": "Hypertension", "synonyms": ["High blood pressure"]},
            {"id": "HP:0001627", "name": "Diabetes", "synonyms": ["Sugar diabetes"]},
            {"id": "HP:0002090", "name": "Asthma", "synonyms": ["Reactive airway disease"]},
            {"id": "HP:0001511", "name": "Obesity", "synonyms": ["Fatty syndrome"]},
            {"id": "HP:0004322", "name": "Anemia", "synonyms": ["Lack of red blood cells"]}
        ]
        self.sample_mapping_file = 'examples/sample_mapping.json'
        with open(self.sample_mapping_file, 'w') as f:
            json.dump({
                "Hypertension": "HP:0000822",
                "Diabetes": "HP:0001627",
                "Asthma": "HP:0002090"
            }, f)

    def tearDown(self):
        import os
        os.remove(self.sample_mapping_file)

    def test_fetch_hpo_terms(self):
        # Create a temporary HPO terms file
        temp_hpo_file = 'temp_hpo_terms.json'
        with open(temp_hpo_file, 'w') as f:
            json.dump(self.hpo_terms, f)
        
        fetched_terms = fetch_hpo_terms(local_file_path=temp_hpo_file)
        self.assertEqual(len(fetched_terms), 5)
        self.assertEqual(fetched_terms[0]['name'], "Hypertension")
        
        # Clean up
        import os
        os.remove(temp_hpo_file)

    def test_map_to_hpo_with_custom_mapping(self):
        custom_mappings = load_custom_mappings(self.sample_mapping_file)
        hpo_id = map_to_hpo("Hypertension", self.hpo_terms, custom_mappings)
        self.assertEqual(hpo_id, "HP:0000822")

    def test_map_to_hpo_without_custom_mapping(self):
        hpo_id = map_to_hpo("Asthma", self.hpo_terms)
        self.assertEqual(hpo_id, "HP:0002090")

    def test_map_to_hpo_with_synonym(self):
        hpo_id = map_to_hpo("High blood pressure", self.hpo_terms)
        self.assertEqual(hpo_id, "HP:0000822")

    def test_map_to_hpo_unmapped_term(self):
        hpo_id = map_to_hpo("Unknown Phenotype", self.hpo_terms)
        self.assertIsNone(hpo_id)

if __name__ == '__main__':
    unittest.main()