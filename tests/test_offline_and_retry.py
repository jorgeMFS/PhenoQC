import os
import types
import builtins
import pytest

from phenoqc.mapping import OntologyMapper


def _base_config(url: str, offline: bool = False):
    return {
        'ontologies': {
            'HPO': {
                'name': 'Human Phenotype Ontology',
                'source': 'url',
                'url': url,
                'format': 'obo',
            }
        },
        'default_ontologies': ['HPO'],
        'cache_expiry_days': 30,
        'offline': offline,
    }


def test_offline_mode_raises_when_cache_missing(tmp_path, monkeypatch):
    # Point cache to a temp directory with no cached ontologies
    monkeypatch.setattr(OntologyMapper, 'CACHE_DIR', str(tmp_path))
    cfg = _base_config(url='http://example.com/hp.obo', offline=True)

    with pytest.raises(FileNotFoundError):
        OntologyMapper(cfg)


def test_offline_mode_uses_cache_when_present(tmp_path, monkeypatch):
    # Prepare a dummy cached file at ~/.phenoqc/ontologies/HPO.obo
    monkeypatch.setattr(OntologyMapper, 'CACHE_DIR', str(tmp_path))
    cached = tmp_path / 'HPO.obo'
    cached.write_text('id: HP:0000001\nname: All\n')

    # Avoid parsing real OBO; stub parse and alt-id scan
    monkeypatch.setattr(OntologyMapper, 'parse_ontology', lambda self, p, fmt: {})
    monkeypatch.setattr(OntologyMapper, '_scan_alt_map_obo', lambda self, p: {})

    cfg = _base_config(url='http://example.com/hp.obo', offline=True)
    mapper = OntologyMapper(cfg)
    assert 'HPO' in mapper.ontologies


def test_retry_backoff_downloads_then_caches(tmp_path, monkeypatch):
    # Point cache to temp directory and ensure no file exists
    monkeypatch.setattr(OntologyMapper, 'CACHE_DIR', str(tmp_path))
    # Stub parsing to avoid heavy pronto parsing
    monkeypatch.setattr(OntologyMapper, 'parse_ontology', lambda self, p, fmt: {})
    monkeypatch.setattr(OntologyMapper, '_scan_alt_map_obo', lambda self, p: {})

    class _Resp:
        def __init__(self, code, content=b'obo'):
            self.status_code = code
            self.content = content

    calls = {'n': 0}

    def fake_get(url, timeout=30):
        calls['n'] += 1
        # First two attempts fail, third succeeds
        if calls['n'] < 3:
            return _Resp(500)
        return _Resp(200, b'format-version: 1.2\n[Term]\nid: HP:0000001\nname: All\n')

    import phenoqc.mapping as mapping_mod
    monkeypatch.setattr(mapping_mod, 'requests', types.SimpleNamespace(get=fake_get))

    cfg = _base_config(url='http://example.com/hp.obo', offline=False)
    OntologyMapper(cfg)

    # Assert we retried and cached
    assert calls['n'] >= 3
    assert (tmp_path / 'HPO.obo').exists()


def test_cli_offline_reaches_batch_process(tmp_path, monkeypatch):
    # Monkeypatch collect_files to avoid FS dependencies
    import phenoqc.cli as cli_mod

    monkeypatch.setattr(cli_mod, 'collect_files', lambda paths, recursive=False: ['/tmp/dummy.csv'])

    seen = {}

    def fake_batch_process(**kwargs):
        seen.update(kwargs)
        return []

    monkeypatch.setattr(cli_mod, 'batch_process', fake_batch_process)

    argv = [
        'phenoqc',
        '--input', 'dummy.csv',
        '--schema', 'schema.json',
        '--config', 'config.yaml',
        '--unique_identifiers', 'ID',
        '--offline',
    ]
    monkeypatch.setattr('sys.argv', argv)

    # Run main; should not raise
    cli_mod.main()
    assert seen.get('offline') is True


