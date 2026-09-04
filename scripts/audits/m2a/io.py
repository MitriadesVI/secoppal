"""Frozen inputs, provenance and volume arithmetic."""
from collections import defaultdict
import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
M0 = ROOT / 'artifacts/audits/municipal_m0_v1'
FINAL = ROOT / 'artifacts/audits/municipal_m1a_final'
M1B = ROOT / 'artifacts/audits/municipal_m1b'
M1A2A = ROOT / 'artifacts/audits/municipal_m1a_2a'
DS = ('p6dx-8zbt', 'jbjy-vk9h')
ACC = 'ACCREDITED_MUNICIPAL_AFFILIATION'
BOGOTA = '11001'
BOGOTA_AUTHORITY = 'DECLARED_BOGOTA_EXECUTIVE_REGISTRY_V1'
CONTRACT_BASIS = 'DERIVED_FROM_GLOBAL_IDENTITY'
METRICS = ('process_physical_rows', 'unique_processes', 'contract_physical_rows', 'unique_contracts')


def read(path):
    raw = path.read_bytes()
    return json.loads(gzip.decompress(raw) if path.suffix == '.gz' else raw)


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + '\n')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stable_id(kind, *parts):
    return kind + ':' + hashlib.sha256(json.dumps(parts, ensure_ascii=False).encode()).hexdigest()[:20]


def protected_hashes():
    paths = set((ROOT / 'app').rglob('*'))
    for folder in (M0, FINAL, M1B, M1A2A):
        paths.update(folder.rglob('*'))
    return {str(p.relative_to(ROOT)): digest(p) for p in sorted(paths)
            if p.is_file() and '__pycache__' not in str(p) and p.suffix != '.pyc'}


def byte_identity(before):
    after = protected_hashes()
    changed = sorted(p for p in before.keys() | after.keys() if before.get(p) != after.get(p))
    return dict(status='FAIL' if changed else 'PASS', changed=changed,
                files={p: dict(before=before.get(p), after=after.get(p))
                       for p in sorted(before.keys() | after.keys())})


def volumes(rows):
    return dict(process_physical_rows=sum(r['physical_rows'] for r in rows if r['dataset'] == DS[0]),
                unique_processes=sum(r['unique_ids'] for r in rows if r['dataset'] == DS[0]),
                contract_physical_rows=sum(r['physical_rows'] for r in rows if r['dataset'] == DS[1]),
                unique_contracts=sum(r['physical_rows'] for r in rows if r['dataset'] == DS[1]),
                unique_contracts_provenance=CONTRACT_BASIS,
                process_physical_rows_provenance='MEASURED', unique_processes_provenance='MEASURED',
                contract_physical_rows_provenance='MEASURED')


def sum_volumes(items):
    return {**{k: sum(p['volumes'][k] for p in items) for k in METRICS},
            'unique_contracts_provenance': CONTRACT_BASIS}


def reconciliation(analysis):
    rows = analysis['strata']
    recognized = {r['municipality'] for r in rows if r['covered']}
    uncovered = [r for r in rows if r['status'] == ACC and not r['covered']]
    cuts = {'all': uncovered, 'inside_recognized': [r for r in uncovered if r['municipality'] in recognized],
            'residual_36': [r for r in uncovered if r['municipality'] not in recognized]}
    result = {k: dict(volumes=volumes(rs), profiles=len({(r['account'], r['name']) for r in rs}),
                      territories=len({r['municipality'] for r in rs})) for k, rs in cuts.items()}
    expected = {'all': (15785, 12281, 138, 96), 'inside_recognized': (13272, 10570, 99, 60),
                'residual_36': (2513, 1711, 39, 36)}
    for k, (processes, contracts, profiles, towns) in expected.items():
        v = result[k]
        if (v['volumes']['unique_processes'], v['volumes']['contract_physical_rows'],
                v['profiles'], v['territories']) != (processes, contracts, profiles, towns):
            raise ValueError(f'STOP: residual reconciliation differs: {k}: {v}')
    return result


def physical_profiles(analysis):
    grouped = defaultdict(list)
    for i, r in enumerate(analysis['strata']):
        grouped[r['account'], r['name']].append((i, r))
    return [dict(profile_id=stable_id('physical', account, name), account=account,
                 physical_source_name=name, fiscal_identity=sorted({r['nit_evidence'] for _, r in rows}),
                 stratum_indices=[i for i, _ in rows], volumes=volumes([r for _, r in rows]))
            for (account, name), rows in sorted(grouped.items())]
