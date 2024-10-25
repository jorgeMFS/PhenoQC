import os
from typing import Dict, List, Any, Optional
import yaml
import pronto  # Library for parsing ontologies
from rapidfuzz import fuzz, process  # For fuzzy matching

class OntologyMapper:
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initializes the OntologyMapper by loading ontologies from the configuration file.

        Args:
            config_path (str): Path to the configuration YAML file.
        """
        self.config = self.load_config(config_path)
        self.ontologies = self.load_ontologies()
        self.default_ontologies = self.config.get('default_ontologies', [])
        if not self.default_ontologies:
            raise ValueError("No default ontologies specified in the configuration.")
        self.fuzzy_threshold = self.config.get('fuzzy_threshold', 80)  # Default threshold

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
                print(f"Loading ontology '{ontology_id}' from '{ontology_file}'...")
                ontologies[ontology_id] = self.parse_ontology(ontology_file)
            else:
                raise FileNotFoundError(f"Ontology file '{ontology_file}' for '{ontology_id}' not found.")
        return ontologies

    def parse_ontology(self, ontology_file: str) -> Dict[str, str]:
        """
        Parses an ontology file into a mapping dictionary.

        Args:
            ontology_file (str): Path to the ontology file.

        Returns:
            dict: Mapping from term names and synonyms to their standardized IDs.
        """
        mapping = {}
        onto = pronto.Ontology(ontology_file)
        for term in onto.terms():
            term_id = term.id
            term_name = term.name.lower().strip() if term.name else ''
            synonyms = [syn.description.lower().strip() for syn in term.synonyms]
            terms_to_map = [term_name] + synonyms
            for t in terms_to_map:
                if t:
                    mapping[t] = term_id
        return mapping

    def map_term(self, term: str, target_ontologies: Optional[List[str]] = None, custom_mappings: Optional[Dict[str, str]] = None) -> Dict[str, Optional[str]]:
        """
        Maps a phenotypic term to IDs in the specified ontologies.

        Args:
            term (str): Phenotypic term to map.
            target_ontologies (list, optional): List of ontology identifiers to map to.
                If None, maps to the default ontologies.
            custom_mappings (dict, optional): Custom mappings for terms.

        Returns:
            dict: Dictionary with ontology IDs mapped for the term.
        """
        if target_ontologies is None:
            target_ontologies = self.default_ontologies

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

            if not mapped_id:
                # Perform fuzzy matching
                match, score, _ = process.extractOne(
                    term_lower, ontology_mapping.keys(), scorer=fuzz.token_sort_ratio
                )
                if score >= self.fuzzy_threshold:
                    mapped_id = ontology_mapping[match]
                else:
                    mapped_id = None  # No suitable match found

            mappings[ontology_id] = mapped_id
        return mappings

    def map_terms(self, terms: List[str], target_ontologies: Optional[List[str]] = None, custom_mappings: Optional[Dict[str, str]] = None) -> Dict[str, Dict[str, Optional[str]]]:
        """
        Maps a list of phenotypic terms to IDs in the specified ontologies.

        Args:
            terms (list): List of phenotypic terms to map.
            target_ontologies (list, optional): List of ontology identifiers to map to.
                If None, maps to the default ontologies.
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
