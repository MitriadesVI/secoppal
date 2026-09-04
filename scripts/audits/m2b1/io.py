"""Load M2B as frozen authority and reconcile the targeted M2B.1 universe."""
from collections import defaultdict
from pathlib import Path

from scripts.audits.m2a.io import ROOT, M0, read, save, digest, sum_volumes

M2B = ROOT / 'artifacts/audits/municipal_m2b'
OUT = ROOT / 'artifacts/audits/municipal_m2b_1'
TARGETS = {'F': (444, 168407, 28436), 'C': (29, 15945, 565),
           'I': (20, 16935, 1852), 'H': (59, 2646, 2412)}


def validate_output(out):
    out = Path(out).resolve()
    if out == ROOT or ROOT in out.parents:
        if out != OUT.resolve():
            raise ValueError('M2B.1 output inside repository must be its dedicated audit directory')
    if out in ROOT.parents:
        raise ValueError('M2B.1 output cannot be an ancestor of the repository')
    return out


def protected_hashes():
    paths = set((ROOT / 'app').rglob('*'))
    for folder in (ROOT / 'artifacts/audits').glob('municipal_m*'):
        if folder.name != 'municipal_m2b_1':
            paths.update(folder.rglob('*'))
    for folder in (ROOT / 'scripts/audits/m2a', ROOT / 'scripts/audits/m2b'):
        paths.update(folder.rglob('*'))
    for path in (ROOT / 'docs/gazetteer/MUNICIPAL_M2A.md', ROOT / 'docs/gazetteer/MUNICIPAL_M2B.md',
                 ROOT / 'docs/decisions/2026-09-02_ADR-074_institutional-buyer-unit-and-group.md',
                 ROOT / 'docs/decisions/2026-09-04_ADR-075_municipal-territorial-affiliation-evidence-chains.md',
                 ROOT / 'docs/decisions/2026-09-04_ADR-076_municipal-own-sigep-ese-identity.md'):
        paths.add(path)
    return {str(p.relative_to(ROOT)): digest(p) for p in sorted(paths)
            if p.is_file() and '__pycache__' not in str(p) and p.suffix != '.pyc'}


def byte_identity(before):
    after = protected_hashes()
    changed = sorted(p for p in before.keys() | after.keys() if before.get(p) != after.get(p))
    return dict(status='FAIL' if changed else 'PASS', changed=changed,
                files={p: dict(before=before.get(p), after=after.get(p))
                       for p in sorted(before.keys() | after.keys())})


def summary(profiles):
    volumes = sum_volumes(profiles)
    return dict(profiles=len(profiles), profile_ids=sorted(p['profile_id'] for p in profiles),
                unique_processes=volumes['unique_processes'], contract_rows=volumes['contract_physical_rows'],
                territories=sorted({p['territory_divipola'] for p in profiles}),
                families=sorted({p['institutional_family'] for p in profiles}))


def load():
    pending = read(M2B / 'pending_identity_profiles.json')['profiles']
    by_id = {p['profile_id']: p for p in pending}
    taxonomy = read(M2B / 'identity_failure_taxonomy.json')
    primary = {r['profile_id']: r['primary'] for r in taxonomy['profiles']}
    via = read(M2B / 'identity_vias.json')
    decisions = {d['profile_id']: d for d in via['profiles']}
    accepted = read(M2B / 'adjudicated_candidates.json')['candidates']
    accepted_ids = {d['profile_id'] for d in accepted}
    target_all, target_new = {}, {}
    for label, expected in TARGETS.items():
        ps = [p for p in pending if primary[p['profile_id']] == label]
        actual = (len(ps), summary(ps)['unique_processes'], summary(ps)['contract_rows'])
        if actual != expected:
            raise ValueError(f'STOP: frozen M2B class {label} mismatch: {actual} != {expected}')
        target_all[label] = ps
        target_new[label] = [p for p in ps if p['profile_id'] not in accepted_ids]
    if len(accepted_ids) != 55 or len(pending) - len(accepted_ids) != 876:
        raise ValueError('STOP: M2B accepted/pending reconciliation changed')
    if sum(len(v) for v in target_new.values()) != 497:
        raise ValueError('STOP: targeted delta universe must contain 497 profiles')
    sigep = read(M0 / 'sources/sigep.json.gz')
    for s in sigep:
        s['divipola'] = s['DIVIPOLA DEPARTAMENTO'].zfill(2) + s['DIVIPOLA MUNICIPIO'].zfill(3)
    directory_rows = read(M0 / 'sources/directory.json.gz')['rows']
    all_profiles = read(ROOT / 'artifacts/audits/municipal_m2a/physical_profiles.json')['profiles']
    return dict(pending=pending, by_id=by_id, primary=primary, m2b_decisions=decisions,
                m2b_accepted=accepted, accepted_ids=accepted_ids, target_all=target_all, target_new=target_new,
                target_profiles=[p for v in target_new.values() for p in v], sigep=sigep,
                directory=directory_rows, all_profiles=all_profiles,
                all_by_id={p['profile_id']: p for p in all_profiles},
                aliases=read(ROOT / 'app/data/aliases_db.json'),
                towns={t['divipola']: t for t in read(ROOT / 'app/data/municipal_territories_v1.json')['territories']})


def indexes(ctx):
    ix = {k: defaultdict(list) for k in ('sigep_territory', 'sigep_code', 'directory_account',
                                         'directory_fiscal', 'directory_territory', 'profile_fiscal',
                                         'profile_territory', 'directory_base', 'profile_base', 'canonical_base')}
    from scripts.audits.m2b.identity_vias import ese_core
    for i, s in enumerate(ctx['sigep']):
        ix['sigep_territory'][s['divipola']].append(i)
        ix['sigep_code'][s['CÓDIGO SIGEP']].append(i)
    from scripts.audits.municipal_directory_authority import fiscal, location_code
    for i, r in enumerate(ctx['directory']):
        ix['directory_account'][str(r.get('codigo_entidad', ''))].append(i)
        ix['directory_territory'][location_code(r)].append(i)
        ix['directory_base'][ese_core(r.get('nombre_entidad', ''))[0]].append(i)
        if fiscal(r.get('nit_entidad', '')):
            ix['directory_fiscal'][fiscal(r.get('nit_entidad', ''))].append(i)
    for p in ctx['all_profiles']:
        ix['profile_territory'][p.get('territory_divipola')].append(p['profile_id'])
        ix['profile_base'][ese_core(p['physical_source_name'])[0]].append(p['profile_id'])
        for nit in {fiscal(n) for n in p['fiscal_identity']}:
            if nit:
                ix['profile_fiscal'][nit].append(p['profile_id'])
    for row in ctx['aliases']:
        ix['canonical_base'][ese_core(row['official_name'])[0]].append(row['official_name'])
    ix['explain_cache'] = {}
    return ix
