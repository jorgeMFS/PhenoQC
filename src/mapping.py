import requests
import json

def fetch_hpo_terms(local_file_path='hpo_terms.json'):
    """
    Fetches HPO terms from a local JSON file.
    
    Args:
        local_file_path (str): Path to the local HPO JSON file.
    
    Returns:
        list: List of HPO terms.
    """
    with open(local_file_path, 'r') as f:
        hpo_terms = json.load(f)
    return hpo_terms

def map_to_hpo(term, hpo_terms, custom_mappings=None):
    """
    Maps a phenotypic term to an HPO ID.
    
    Args:
        term (str): Phenotypic term to map.
        hpo_terms (list): List of HPO terms.
        custom_mappings (dict, optional): Custom mappings for terms.
    
    Returns:
        str or None: Mapped HPO ID or None if not found.
    """
    # Check custom mappings first
    if custom_mappings and term in custom_mappings:
        return custom_mappings[term]
    
    # Search in HPO terms
    term_lower = term.lower()
    for hpo in hpo_terms:
        if term_lower == hpo['name'].lower() or term_lower in [syn.lower() for syn in hpo.get('synonyms', [])]:
            return hpo['id']
    return None  # Unmapped term

def load_custom_mappings(mapping_file):
    """
    Loads custom term mappings from a JSON file.
    
    Args:
        mapping_file (str): Path to the custom mapping JSON file.
    
    Returns:
        dict: Custom mappings.
    """
    with open(mapping_file, 'r') as f:
        custom_mappings = json.load(f)
    return custom_mappings