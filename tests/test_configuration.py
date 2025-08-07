import unittest
import tempfile
import os
import json
import yaml

from phenoqc.configuration import load_config, save_config


class TestConfigurationModule(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config = {"param1": 10, "param2": [1, 2, 3]}

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_config_yaml_and_json(self):
        yaml_path = os.path.join(self.temp_dir.name, "config.yaml")
        json_path = os.path.join(self.temp_dir.name, "config.json")
        with open(yaml_path, "w") as f:
            yaml.safe_dump(self.config, f)
        with open(json_path, "w") as f:
            json.dump(self.config, f)

        self.assertEqual(load_config(yaml_path), self.config)
        self.assertEqual(load_config(json_path), self.config)

        with open(yaml_path, "r") as f:
            self.assertEqual(load_config(f), self.config)
        with open(json_path, "r") as f:
            self.assertEqual(load_config(f), self.config)

    def test_save_config_yaml_and_json(self):
        yaml_path = os.path.join(self.temp_dir.name, "saved.yaml")
        json_path = os.path.join(self.temp_dir.name, "saved.json")
        save_config(self.config, yaml_path)
        save_config(self.config, json_path)

        with open(yaml_path) as f:
            self.assertEqual(yaml.safe_load(f), self.config)
        with open(json_path) as f:
            self.assertEqual(json.load(f), self.config)

    def test_invalid_extension(self):
        invalid_path = os.path.join(self.temp_dir.name, "config.txt")
        with open(invalid_path, "w") as f:
            f.write("test")
        with self.assertRaises(ValueError):
            load_config(invalid_path)
        with self.assertRaises(ValueError):
            save_config(self.config, invalid_path)


if __name__ == "__main__":
    unittest.main()
