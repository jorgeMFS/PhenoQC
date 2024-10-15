import requests
import json
import os
from typing import Dict, List, Any
import yaml

class OntologyMapper:
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initializes the OntologyMapper by loading ontologies from the configuration file.
        
        Args:
            config_path (str): Path to the configuration YAML file.
        """
        self.config = self.load_config(config_path)
        self.ontologies = self.load_ontologies()
        self.default_ontology = self.config.get('default_ontology', 'HPO')
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Loads the YAML configuration file.
        
        Args:
            config_path (str): Path to the configuration YAML file.
        
        Returns:
            dict: Configuration parameters.
        """
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def load_ontologies(self) -> Dict[str, Dict[str, str]]:
        """
        Loads all ontologies specified in the configuration file.
        
        Returns:
            dict: A dictionary where keys are ontology identifiers and values are term mapping dictionaries.
        """
        ontologies = {}
        ontology_configs = self.config.get('ontologies', {})
        for ontology_id, ontology_info in ontology_configs.items():
            ontology_file = ontology_info.get('file')
            if ontology_file and os.path.exists(ontology_file):
                with open(ontology_file, 'r') as f:
                    ontology_data = json.load(f)
                    ontologies[ontology_id] = self.parse_ontology(ontology_data)
            else:
                raise FileNotFoundError(f"Ontology file '{ontology_file}' for '{ontology_id}' not found.")
        return ontologies
    
    def parse_ontology(self, ontology_data: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Parses ontology data into a mapping dictionary.
        
        Args:
            ontology_data (list): List of ontology terms.
        
        Returns:
            dict: Mapping from term names and synonyms to their standardized IDs.
        """
        mapping = {}
        for term in ontology_data:
            term_id = term.get('id')
            term_name = term.get('name', '').lower()
            synonyms = [syn.lower() for syn in term.get('synonyms', [])]
            if term_name:
                mapping[term_name] = term_id
            for syn in synonyms:
                mapping[syn] = term_id
        return mapping
    
    def map_term(self, term: str, target_ontologies: List[str] = None, custom_mappings: Dict[str, str] = None) -> Dict[str, str]:
        """
        Maps a phenotypic term to IDs in the specified ontologies.
        
        Args:
            term (str): Phenotypic term to map.
            target_ontologies (list, optional): List of ontology identifiers to map to.
                If None, maps to the default ontology.
            custom_mappings (dict, optional): Custom mappings for terms.
        
        Returns:
            dict: Dictionary with ontology IDs mapped for the term.
        """
        if target_ontologies is None:
            target_ontologies = [self.default_ontology]
        
        term_lower = term.lower().strip()
        mappings = {}
        
        # Check custom mappings first
        if custom_mappings and term_lower in custom_mappings:
            custom_id = custom_mappings[term_lower]
            for ontology_id in target_ontologies:
                mappings[ontology_id] = custom_id
            return mappings
        
        for ontology_id in target_ontologies:
            ontology_mapping = self.ontologies.get(ontology_id, {})
            mapped_id = ontology_mapping.get(term_lower, None)
            mappings[ontology_id] = mapped_id
        return mappings
    
    def map_terms(self, terms: List[str], target_ontologies: List[str] = None, custom_mappings: Dict[str, str] = None) -> Dict[str, Dict[str, str]]:
        """
        Maps a list of phenotypic terms to IDs in the specified ontologies.
        
        Args:
            terms (list): List of phenotypic terms to map.
            target_ontologies (list, optional): List of ontology identifiers to map to.
                If None, maps to the default ontology.
            custom_mappings (dict, optional): Custom mappings for terms.
        
        Returns:
            dict: Nested dictionary {term: {ontology_id: mapped_id}}.
        """
        mappings = {}
        for term in terms:
            mappings[term] = self.map_term(term, target_ontologies, custom_mappings)
        return mappings
    
    def get_supported_ontologies(self) -> List[str]:
        """
        Retrieves a list of supported ontology identifiers.
        
        Returns:
            list: Supported ontology identifiers.
        """
        return list(self.ontologies.keys())
