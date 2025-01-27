import pandas as pd
import re

def suggest_ontologies(column_name: str, column_data: pd.Series, available_ontologies: dict) -> list:
    """
    Intelligently suggest ontologies for a column based on:
    1. Column name patterns
    2. Data content analysis
    3. Data type
    4. Known ontology patterns
    
    Args:
        column_name: Name of the column
        column_data: Pandas Series containing column data
        available_ontologies: Dictionary of available ontologies and their metadata
    
    Returns:
        list: Suggested ontology IDs
    """
    suggestions = set()
    
    # Normalize column name for matching
    col_lower = column_name.lower()
    
    # 1. Column name pattern matching
    name_patterns = {
        'phenotype': ['HPO', 'MPO'],
        'disease': ['DO', 'MONDO'],
        'symptom': ['HPO'],
        'diagnosis': ['DO', 'ICD'],
        'clinical': ['HPO'],
        'pathology': ['DO'],
        'genetic': ['GO'],
        'molecular': ['GO'],
        'anatomy': ['UBERON']
    }
    
    for pattern, onts in name_patterns.items():
        if pattern in col_lower:
            suggestions.update([ont for ont in onts if ont in available_ontologies])
    
    # 2. Data content analysis
    if not column_data.empty:
        sample_values = column_data.dropna().astype(str).unique()[:100]  # Analyze up to 100 unique values
        
        # Check for ontology ID patterns
        patterns = {
            'HPO': r'HP:[0-9]{7}',
            'DO': r'DOID:[0-9]+',
            'MPO': r'MP:[0-9]{7}',
            'GO': r'GO:[0-9]{7}',
            'MONDO': r'MONDO:[0-9]{7}'
        }
        
        for value in sample_values:
            for ont, pattern in patterns.items():
                if re.search(pattern, value) and ont in available_ontologies:
                    suggestions.add(ont)
        
        # Check for common term patterns
        term_indicators = {
            'HPO': ['abnormal', 'phenotype', 'clinical', 'syndrome'],
            'DO': ['disease', 'disorder', 'syndrome', 'condition'],
            'MPO': ['abnormal', 'phenotype', 'mutant'],
        }
        
        for value in sample_values:
            value_lower = value.lower()
            for ont, indicators in term_indicators.items():
                if ont in available_ontologies and any(ind in value_lower for ind in indicators):
                    suggestions.add(ont)
    
    # 3. Data type checks
    if column_data.dtype == object:  # String/categorical data
        if not suggestions:  # If no specific matches found, suggest common ontologies
            suggestions.update(['HPO', 'DO'])  # Default to common clinical ontologies
    
    # 4. Filter suggestions to only include available ontologies
    return sorted(list(suggestions)) 