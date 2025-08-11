import os
from typing import Dict, List, Any, Optional

import yaml
import pronto
from rapidfuzz import fuzz, process
import requests
import time
import re
import unicodedata

from .configuration import load_config
from .logging_module import log_activity
from datetime import datetime, timedelta

class OntologyMapper:
    CACHE_DIR = os.path.expanduser("~/.phenoqc/ontologies")

    PREFIX_ALIASES = {
        'hp': 'HPO', 'hpo': 'HPO',
        'do': 'DO', 'doid': 'DO',
        'mp': 'MPO', 'mpo': 'MPO',
        'go': 'GO', 'mondo': 'MONDO', 'efo': 'EFO', 'mesh': 'MESH',
    }

    DEFAULT_ID_REGEX = {
        'HPO': re.compile(r"(?i)\b(?:hp|hpo)[:_\s]?(\d{5,7})\b"),
        'DO': re.compile(r"(?i)\b(?:doid|do)[:_\s]?(\d+)\b"),
        'MPO': re.compile(r"(?i)\b(?:mp|mpo)[:_\s]?(\d+)\b"),
        'GO': re.compile(r"(?i)\bgo[:_\s]?(\d{7})\b"),
        'MONDO': re.compile(r"(?i)\bmondo[:_\s]?(\d+)\b"),
        'EFO': re.compile(r"(?i)\befo[:_\s]?(\d+)\b"),
        'MESH': re.compile(r"(?i)\bmesh[:_\s]?(\w+)\b"),
    }

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
        self.offline = bool(self.config.get('offline', False))
        self._id_regex = self._build_id_regex_from_config(self.config)
        # Alt-to-primary remapping (per ontology) from OBO scan
        # needs to exist before ontologies are loaded because the loader
        # populates this structure while parsing the ontology files
        self._alt_to_primary: Dict[str, Dict[str, str]] = {}
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

    def _build_id_regex_from_config(self, cfg: dict) -> Dict[str, re.Pattern]:
        patterns = dict(self.DEFAULT_ID_REGEX)
        try:
            for onto_id, onto_cfg in (cfg.get('ontologies', {}) or {}).items():
                patt = onto_cfg.get('id_pattern')
                if patt:
                    try:
                        patterns[onto_id] = re.compile(patt, re.IGNORECASE)
                    except re.error:
                        pass
        except Exception:
            pass
        return patterns

    @staticmethod
    def _normalize_text(text: str) -> str:
        if text is None:
            return ""
        t = unicodedata.normalize('NFKC', str(text)).lower().strip()
        t = t.replace('\u200b', ' ')
        t = re.sub(r"[\n\t]", " ", t)
        t = re.sub(r"\s+", " ", t)
        t = re.sub(r"[()\[\]{}]", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        return t

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
                    # Build alt->primary index
                    self._alt_to_primary[ontology_id] = self._scan_alt_map_obo(ontology_file)
                else:
                    raise FileNotFoundError(f"Ontology file '{ontology_file}' for '{ontology_id}' not found.")
            elif source == 'url':
                url = ontology_info.get('url')
                file_format = ontology_info.get('format')
                if url and file_format:
                    print(f"Loading ontology '{ontology_id}' from cache or URL...")
                    ontology_file_path = self.fetch_ontology_file_with_cache(ontology_id, url, file_format)
                    ontologies[ontology_id] = self.parse_ontology(ontology_file_path, file_format)
                    self._alt_to_primary[ontology_id] = self._scan_alt_map_obo(ontology_file_path)
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

        if self.offline:
            # Offline: do not download, fail fast if cache missing/expired
            raise FileNotFoundError(
                f"Offline mode is enabled and cached ontology for '{ontology_id}' was not found or is expired at '{cached_file_path}'."
            )

        # Download the ontology with simple retry/backoff and save to cache
        print(f"Downloading ontology '{ontology_id}' from '{url}'...")
        retries = 3
        backoff = 2.0
        last_exc = None
        for i in range(retries):
            try:
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    with open(cached_file_path, 'wb') as f:
                        f.write(response.content)
                    print(f"Ontology '{ontology_id}' saved to cache at '{cached_file_path}'")
                    return cached_file_path
                else:
                    last_exc = Exception(
                        f"Failed to download ontology '{ontology_id}' from '{url}'. Status code: {response.status_code}"
                    )
            except Exception as e:
                last_exc = e
            # Backoff before next attempt
            if i < retries - 1:
                time.sleep(backoff ** i)
        # After retries, raise the last exception
        raise last_exc if last_exc else Exception(f"Failed to download ontology '{ontology_id}' from '{url}'.")

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
            # Also index by the identifier itself (lowercased), so incoming IDs map directly
            id_key = (term_id or '').lower().strip()
            # alt_id support (multiple sources depending on pronto version)
            alt_ids = []
            try:
                if hasattr(term, 'other') and isinstance(term.other, dict):
                    alt_ids.extend([str(x).lower().strip() for x in term.other.get('alt_id', [])])
            except Exception:
                pass
            try:
                alt_prop = getattr(term, 'alternate_ids', None)
                if alt_prop:
                    if isinstance(alt_prop, (list, set, tuple)):
                        alt_ids.extend([str(x).lower().strip() for x in alt_prop])
                    else:
                        alt_ids.append(str(alt_prop).lower().strip())
            except Exception:
                pass
            try:
                alt_prop2 = getattr(term, 'alt_ids', None)
                if alt_prop2:
                    if isinstance(alt_prop2, (list, set, tuple)):
                        alt_ids.extend([str(x).lower().strip() for x in alt_prop2])
                    else:
                        alt_ids.append(str(alt_prop2).lower().strip())
            except Exception:
                pass
            # xrefs support (index raw xref string and last segment after colon)
            xrefs = []
            try:
                raw_xrefs = [str(x).lower().strip() for x in getattr(term, 'xrefs', [])]
                xrefs = list(raw_xrefs)
                for rx in raw_xrefs:
                    parts = rx.split(":", 1)
                    if len(parts) == 2 and parts[1]:
                        xrefs.append(parts[1])
            except Exception:
                xrefs = []
            terms_to_map = [term_name] + synonyms + [id_key] + alt_ids + xrefs
            for t in terms_to_map:
                if t:
                    mapping[t] = term_id
        # Fallback: add alt_id relationships by scanning OBO if present
        eff_fmt = (file_format or '').lower()
        if not eff_fmt:
            _, ext = os.path.splitext(ontology_file_path.lower())
            if ext == '.obo':
                eff_fmt = 'obo'
        if eff_fmt == 'obo':
            self._augment_alt_ids_from_obo(ontology_file_path, mapping)
        return mapping

    def _augment_alt_ids_from_obo(self, ontology_file_path: str, mapping: Dict[str, str]) -> None:
        """Best-effort fallback: scan OBO text for alt_id -> id relationships and add to mapping."""
        try:
            with open(ontology_file_path, 'r', encoding='utf-8') as f:
                current_id = None
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if line == '[Term]':
                        current_id = None
                        continue
                    if line.startswith('id:'):
                        current_id = line.split(':', 1)[1].strip()
                        continue
                    if line.startswith('alt_id:') and current_id:
                        alt = line.split(':', 1)[1].strip()
                        mapping[self._normalize_text(alt)] = current_id
        except Exception:
            return

    def _scan_alt_map_obo(self, ontology_file_path: str) -> Dict[str, str]:
        """Return alt_id (uppercase) -> primary id mapping by scanning OBO text."""
        result: Dict[str, str] = {}
        try:
            with open(ontology_file_path, 'r', encoding='utf-8') as f:
                current_id = None
                for line in f:
                    line = line.strip()
                    if line == '[Term]':
                        current_id = None
                        continue
                    if line.startswith('id:'):
                        current_id = line.split(':', 1)[1].strip()
                        continue
                    if line.startswith('alt_id:') and current_id:
                        alt = line.split(':', 1)[1].strip()
                        result[alt.upper()] = current_id
        except Exception:
            return {}
        return result

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

        # Normalize text
        term_norm = self._normalize_text(term)
        mappings = {}

        # Custom mappings first (normalized)
        if custom_mappings:
            cm = {self._normalize_text(k): v for k, v in custom_mappings.items()}
            if term_norm in cm:
                for ontology_id in target_ontologies:
                    mappings[ontology_id] = cm[term_norm]
                return mappings

        # Try direct ID extraction (regex + prefix aliases)
        direct = None
        try:
            direct = self._extract_direct_id(term_norm)
        except Exception:
            direct = None

        for ontology_id in target_ontologies:
            ontology_mapping = self.ontologies.get(ontology_id, {})
            if direct and ontology_id in direct:
                # Prefer canonical mapping via ontology table (handles alt_id -> primary id)
                direct_key = self._normalize_text(direct[ontology_id])
                canonical = ontology_mapping.get(direct_key)
                if not canonical:
                    # Try alt->primary remap if present
                    altmap = self._alt_to_primary.get(ontology_id, {})
                    canonical = altmap.get(direct[ontology_id].upper())
                mappings[ontology_id] = canonical if canonical else direct[ontology_id]
                continue
            # exact normalized
            mapped_id = ontology_mapping.get(term_norm)
            if not mapped_id and ontology_mapping:
                extracted = process.extractOne(
                    term_norm,
                    ontology_mapping.keys(),
                    scorer=fuzz.WRatio,
                    score_cutoff=self.fuzzy_threshold,
                    processor=None,
                )
                if extracted is not None:
                    mapped_id = ontology_mapping.get(extracted[0])
            # If 'DO 1612' like variant: try prefix + space
            if not mapped_id:
                m = re.match(r"^([a-z]+)\s+(\w+)$", term_norm)
                if m:
                    pref, core = m.group(1), m.group(2)
                    canon = self.PREFIX_ALIASES.get(pref)
                    if canon == ontology_id:
                        if ontology_id == 'HPO':
                            mapped_id = f"HP:{int(core):07d}" if core.isdigit() else f"HP:{core}"
                        elif ontology_id == 'DO':
                            mapped_id = f"DOID:{core}"
                        elif ontology_id == 'MPO':
                            mapped_id = f"MP:{core}"
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
