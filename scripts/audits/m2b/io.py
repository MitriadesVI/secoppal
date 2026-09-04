"""Read M2A as frozen authority; never recompute territorial affiliation."""
from collections import defaultdict
from pathlib import Path

from scripts.audits.m2a.io import ROOT, M0, FINAL, read, save, digest, sum_volumes

M2A = ROOT / 'artifacts/audits/municipal_m2a'
OUT = ROOT / 'artifacts/audits/municipal_m2b'
EXPECTED = (931, 347163, 117277)


def protected_hashes():
    paths = set((ROOT / 'app').rglob('*'))
    for folder in (ROOT / 'artifacts/audits').glob('municipal_m*'):
        if folder.name != 'municipal_m2b':
            paths.update(folder.rglob('*'))
    paths.update((ROOT / 'scripts/audits/m2a').rglob('*'))
    return {str(p.relative_to(ROOT)): digest(p) for p in sorted(paths)
            if p.is_file() and '__pycache__' not in str(p) and p.suffix != '.pyc'}


def byte_identity(before):
    after = protected_hashes()
    changed = sorted(p for p in before.keys() | after.keys() if before.get(p) != after.get(p))
    return dict(status='FAIL' if changed else 'PASS', changed=changed,
                files={p: dict(before=before.get(p), after=after.get(p))
                       for p in sorted(before.keys() | after.keys())})


def validate_output(out):
    out = Path(out).resolve()
    if out == ROOT or ROOT in out.parents:
        if out != OUT.resolve():
            raise ValueError('M2B output inside repository must be the dedicated M2B directory')
    if out in ROOT.parents:
        raise ValueError('M2B output cannot be an ancestor of the repository')
    return out


def partition(profiles):
    verified = [p for p in profiles if p['affiliation'] == 'VERIFIED']
    pending = [p for p in verified if p['logical_unit_id'] is None]
    logical = [p for p in verified if p['logical_unit_id'] and not p['recognized']]
    measured = (len(pending), sum_volumes(pending)['unique_processes'],
                sum_volumes(pending)['contract_physical_rows'])
    if measured != EXPECTED or len(logical) != 370 or len(verified) != 1929:
        raise ValueError(f'STOP: frozen universe mismatch: {measured}, logical={len(logical)}')
    if len({p['profile_id'] for p in profiles}) != len(profiles):
        raise ValueError('STOP: duplicated profile ID')
    return pending, logical, verified


def load():
    profiles = read(M2A / 'physical_profiles.json')['profiles']
    pending, unrecognized, verified = partition(profiles)
    affiliations = read(M2A / 'territorial_affiliation.json')['affiliations']
    aff = {a['profile_id']: a for a in affiliations}
    for p in profiles:
        a = aff[p['profile_id']]
        if (p['affiliation'], p['territory_divipola']) != (a['affiliation'], a['territory_divipola']):
            raise ValueError('STOP: M2A affiliation/profile inconsistency')
    sigep = read(M0 / 'sources/sigep.json.gz')
    for s in sigep:
        s['divipola'] = s['DIVIPOLA DEPARTAMENTO'].zfill(2) + s['DIVIPOLA MUNICIPIO'].zfill(3)
    logical = read(M2A / 'logical_units.json')
    return dict(profiles=profiles, pending=pending, unrecognized=unrecognized, verified=verified,
                by_id={p['profile_id']: p for p in profiles}, aff=aff, sigep=sigep,
                directory=read(M0 / 'sources/directory.json.gz')['rows'],
                units=logical['units'], decisions={d['profile_id']: d for d in logical['decisions']},
                strata=read(FINAL / 'm0_post_analysis.json.gz')['strata'],
                territories=read(M2A / 'territory_profiles.json')['territories'],
                towns={t['divipola']: t for t in read(ROOT / 'app/data/municipal_territories_v1.json')['territories']},
                aliases=read(ROOT / 'app/data/aliases_db.json'),
                bindings=read(ROOT / 'app/data/buyer_source_names.json')['bindings'])


def index(items, key):
    result = defaultdict(list)
    for item in items:
        result[key(item)].append(item)
    return result


def summary(profiles):
    return dict(profiles=len(profiles), profile_ids=sorted(p['profile_id'] for p in profiles),
                unique_processes=sum_volumes(profiles)['unique_processes'],
                contract_rows=sum_volumes(profiles)['contract_physical_rows'],
                territories=sorted({p['territory_divipola'] for p in profiles if p['territory_divipola']}),
                families=sorted({p['institutional_family'] for p in profiles}))


__all__ = ['ROOT', 'M0', 'M2A', 'OUT', 'read', 'save', 'digest', 'sum_volumes', 'summary']
