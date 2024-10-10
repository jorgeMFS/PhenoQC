import json
import yaml
import os

def load_config(config_file):
    """
    Loads the configuration from a YAML or JSON file.

    Args:
        config_file (str): Path to the configuration file.

    Returns:
        dict: Configuration settings.
    """
    _, ext = os.path.splitext(config_file)
    with open(config_file, 'r') as f:
        if ext.lower() in ['.yaml', '.yml']:
            return yaml.safe_load(f)
        elif ext.lower() == '.json':
            return json.load(f)
        else:
            raise ValueError("Unsupported configuration file format. Use YAML or JSON.")

def save_config(config, config_file):
    """
    Saves the configuration to a YAML or JSON file.

    Args:
        config (dict): Configuration settings.
        config_file (str): Path to save the configuration file.
    """
    _, ext = os.path.splitext(config_file)
    with open(config_file, 'w') as f:
        if ext.lower() in ['.yaml', '.yml']:
            yaml.dump(config, f)
        elif ext.lower() == '.json':
            json.dump(config, f, indent=4)
        else:
            raise ValueError("Unsupported configuration file format. Use YAML or JSON.")