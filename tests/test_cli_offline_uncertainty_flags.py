import json
from phenoqc.cli import parse_arguments


def test_cli_flags_offline_bias_and_mi(monkeypatch):
    argv = [
        'phenoqc', '--input', 'a.csv', '--schema', 'schema.json', '--unique_identifiers', 'id',
        '--offline', '--impute-diagnostics', 'on', '--diag-repeats', '3', '--diag-mask-fraction', '0.2',
        '--bias-psi-threshold', '0.2', '--bias-cramer-threshold', '0.3',
        '--mi-uncertainty', 'on', '--mi-repeats', '4', '--mi-params', json.dumps({'max_iter': 3})
    ]
    monkeypatch.setattr('sys.argv', argv)
    args = parse_arguments()
    assert args.offline is True
    assert args.impute_diagnostics == 'on'
    assert args.diag_repeats == 3
    assert abs(args.diag_mask_fraction - 0.2) < 1e-9
    assert abs(args.bias_psi_threshold - 0.2) < 1e-9
    assert abs(args.bias_cramer_threshold - 0.3) < 1e-9
    assert args.mi_uncertainty == 'on'
    assert args.mi_repeats == 4
    assert isinstance(args.mi_params, dict) and args.mi_params.get('max_iter') == 3

