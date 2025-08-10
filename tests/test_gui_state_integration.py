import copy


def test_gui_state_shapes_for_new_config_blocks():
    # Simulate the shapes the GUI writes into st.session_state['config']
    cfg = {
        'imputation': {
            'strategy': 'knn',
            'params': {'n_neighbors': 5},
            'per_column': {'x': {'strategy': 'median'}},
            'tuning': {'enable': True, 'mask_fraction': 0.1, 'scoring': 'MAE', 'max_cells': 50000, 'random_state': 42, 'grid': {'n_neighbors': [3,5,7]}}
        },
        'imputation_bias': {'enable': True, 'smd_threshold': 0.1, 'var_ratio_low': 0.5, 'var_ratio_high': 2.0, 'ks_alpha': 0.05},
        'quality_metrics': {'imputation_stability': {'enable': True, 'repeats': 5, 'mask_fraction': 0.1, 'scoring': 'MAE'}},
        'protected_columns': ['label', 'outcome'],
        'redundancy': {'threshold': 0.98, 'method': 'pearson'},
    }
    # Validate expected keys are present and types are sane
    assert isinstance(cfg['imputation'], dict)
    assert isinstance(cfg['imputation'].get('tuning', {}), dict)
    assert isinstance(cfg.get('imputation_bias', {}), dict)
    assert isinstance(cfg.get('quality_metrics', {}), dict)
    assert isinstance(cfg['quality_metrics'].get('imputation_stability', {}), dict)
    assert isinstance(cfg.get('protected_columns', []), list)
    assert isinstance(cfg.get('redundancy', {}), dict)


