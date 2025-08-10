import argparse
from phenoqc.cli import parse_arguments


def test_cli_protected_columns_commas_and_spaces(monkeypatch):
    argv = [
        'phenoqc',
        '--input', 'a.csv',
        '--schema', 'schema.json',
        '--unique_identifiers', 'id',
        '--protected-columns', 'label,outcome', 'group'
    ]
    monkeypatch.setattr('sys.argv', argv)
    args = parse_arguments()
    assert args.protected_columns == ['label', 'outcome', 'group']


def test_cli_protected_columns_duplicates_whitespace(monkeypatch):
    argv = [
        'phenoqc',
        '--input', 'a.csv',
        '--schema', 'schema.json',
        '--unique_identifiers', 'id',
        '--protected-columns', ' label , outcome ', 'label', '  outcome  ', ' '
    ]
    monkeypatch.setattr('sys.argv', argv)
    args = parse_arguments()
    # Normalization keeps order of appearance but strips empties
    assert args.protected_columns == ['label', 'outcome', 'label', 'outcome']


