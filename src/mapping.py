import os
from typing import Dict, List, Any, Optional

import yaml
import pronto
from rapidfuzz import fuzz, process
import requests

from .configuration import load_config
from .logging_module import log_activity
from datetime import datetime, timedelta

class OntologyMapper:
    CACHE_DIR = os.path.expanduser("~/.phenoqc/ontologies")

    def __init__(self, config_source):
        """
        Initializes the OntologyMapper by loading ontologies from a config source.

        Args:
            config_source (Union[str, dict]): Either:
                - A string path to the configuration file (YAML/JSON)
                - An already-loaded dict with configuration data
        """
        if isinstance(config_source, dict):
            # We got a dict directly (e.g., from load_config in the GUI or CLI)
            self.config = config_source
        elif isinstance(config_source, str):
            # We got a path to a config file. Let's load it ourselves:
            self.config = load_config(config_source)
        else:
            raise ValueError(
                "OntologyMapper expects config_source to be either a dict or a path (str). "
                f"Got: {type(config_source)}"
            )

        self.cache_expiry_days = self.config.get('cache_expiry_days', 30)
        self.ontologies = self.load_ontologies()
        self.default_ontologies = self.config.get('default_ontologies', [])
        if not self.default_ontologies:
            raise ValueError("No default ontologies specified in the configuration.")
        self.fuzzy_threshold = self.config.get('fuzzy_threshold', 80)
    
    

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
            source = ontology_info.get('source', 'local').lower()
            if source == 'local':
                ontology_file = ontology_info.get('file')
                if ontology_file and os.path.exists(ontology_file):
                    print(f"Loading ontology '{ontology_id}' from local file '{ontology_file}'...")
                    ontologies[ontology_id] = self.parse_ontology(ontology_file, ontology_info.get('format'))
                else:
                    raise FileNotFoundError(f"Ontology file '{ontology_file}' for '{ontology_id}' not found.")
            elif source == 'url':
                url = ontology_info.get('url')
                file_format = ontology_info.get('format')
                if url and file_format:
                    print(f"Loading ontology '{ontology_id}' from cache or URL...")
                    ontology_file_path = self.fetch_ontology_file_with_cache(ontology_id, url, file_format)
                    ontologies[ontology_id] = self.parse_ontology(ontology_file_path, file_format)
                else:
                    raise ValueError(f"URL or format not specified for ontology '{ontology_id}' in configuration.")
            else:
                raise ValueError(f"Unknown source '{source}' for ontology '{ontology_id}'.")
        return ontologies

    def fetch_ontology_file_with_cache(self, ontology_id: str, url: str, file_format: str) -> str:
        """
        Fetches the ontology file from the cache or downloads it if not present or expired.

        Args:
            ontology_id (str): The ontology identifier.
            url (str): The URL to download the ontology from.
            file_format (str): The format of the ontology file ('obo', 'owl', 'json').

        Returns:
            str: Path to the saved ontology file.
        """
        # Ensure cache directory exists
        os.makedirs(self.CACHE_DIR, exist_ok=True)

        # Construct the cached file path
        cached_file_path = os.path.join(self.CACHE_DIR, f"{ontology_id}.{file_format.lower()}")

        # Check if the cached file exists and is not expired
        if os.path.exists(cached_file_path):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(cached_file_path))
            if datetime.now() - file_mod_time < timedelta(days=self.cache_expiry_days):
                print(f"Using cached ontology file for '{ontology_id}' at '{cached_file_path}'")
                return cached_file_path
            else:
                print(f"Cached ontology file for '{ontology_id}' is expired. Downloading new version...")

        # Download the ontology and save to cache
        print(f"Downloading ontology '{ontology_id}' from '{url}'...")
        response = requests.get(url)
        if response.status_code == 200:
            with open(cached_file_path, 'wb') as f:
                f.write(response.content)
            print(f"Ontology '{ontology_id}' saved to cache at '{cached_file_path}'")
            return cached_file_path
        else:
            raise Exception(f"Failed to download ontology '{ontology_id}' from '{url}'. Status code: {response.status_code}")

    def parse_ontology(self, ontology_file_path: str, file_format: str) -> Dict[str, str]:
        """
        Parses an ontology file into a mapping dictionary.

        Args:
            ontology_file_path (str): Path to the ontology file.
            file_format (str): The format of the ontology file ('obo', 'owl', 'json').

        Returns:
            dict: Mapping from term names and synonyms to their standardized IDs.
        """
        mapping = {}
        print(f"Parsing ontology file '{ontology_file_path}'...")
        onto = pronto.Ontology(ontology_file_path)
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

        # Convert to string if it's not None or numeric
        if term is None:
            term_lower = ""
        else:
            term_lower = str(term).strip().lower()
        
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
                match = None
                score = 0
                if ontology_mapping:
                    extracted = process.extractOne(
                        term_lower, ontology_mapping.keys(), scorer=fuzz.token_sort_ratio
                    )
                    if extracted is not None:
                        match, score, _ = extracted
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
