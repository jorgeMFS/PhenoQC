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


def test_cli_protected_columns_empty_and_omitted(monkeypatch):
    # Omitted flag -> defaults to empty list
    argv1 = [
        'phenoqc', '--input', 'a.csv', '--schema', 'schema.json', '--unique_identifiers', 'id'
    ]
    monkeypatch.setattr('sys.argv', argv1)
    args1 = parse_arguments()
    assert args1.protected_columns == []

    # Empty and whitespace-only entries should normalize to empty list
    argv2 = [
        'phenoqc', '--input', 'a.csv', '--schema', 'schema.json', '--unique_identifiers', 'id',
        '--protected-columns', '', '  ', ' , '
    ]
    monkeypatch.setattr('sys.argv', argv2)
    args2 = parse_arguments()
    assert args2.protected_columns == []


