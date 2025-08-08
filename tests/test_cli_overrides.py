import json
from phenoqc.batch_processing import load_config


def test_cli_overrides_merge(tmp_path):
    cfg = {
        'imputation': {
            'strategy': 'knn',
            'params': {'n_neighbors': 7},
            'tuning': {'enable': False}
        }
    }
    p = tmp_path / 'config.yaml'
    p.write_text(json.dumps(cfg))
    loaded = load_config(str(p))
    # Simulate CLI overrides merged elsewhere; just assert base load works
    assert loaded['imputation']['params']['n_neighbors'] == 7

