"""Reproduce the five manual reviews from frozen evidence, without production writes."""
from pathlib import Path
import gzip
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.municipal_territory import get_municipal_territories
from app.utils.money import normalize_text
from scripts.audits.consolidate_turbaco_m1a2a import A, B, ACCOUNT, OUT, verified_history
from scripts.audits.municipal_directory_authority import (
    directory_affiliation, executive, fiscal, validate_cohort,
)
from scripts.audits.promote_municipal_m1a import DATASETS, encode, ownership, read, sha

M0 = ROOT / 'artifacts/audits/municipal_m0_v1'
EXPECTED = {A: '13836', 'ALCALDIA MUNICIPAL DE OCAÑA': '54498',
            'ALCALDIA MUNICIPIO DE CALARCA': '63130', 'ALCALDIA DISTRITAL BARRANCABERMEJA': '68081',
            'MUNICIPIO DE YUMBO VALLE': '76892'}


def save(name, value):
    (OUT / name).write_bytes(encode(value))


def verify_capture(name):
    meta = read(OUT / 'evidence' / name)
    assert meta['status_code'] == 200
    raw = gzip.decompress((OUT / 'evidence' / name.replace('.json', '.body.gz')).read_bytes())
    assert sha(raw) == meta['sha256'] and json.loads(raw) == meta['rows']
    return meta


def official_sigep_affiliation(record, rows, directory, sigep, corroboration):
    """Case-reviewed SIGEP reconciliation, without requiring directory city.

    This is an audit witness only, not an automatic new runtime authority.
    The supplementary official version must attest an Alcaldía and municipality.
    """
    owner = record['official_name']
    assert executive(owner) and {r['dataset'] for r in rows} == set(DATASETS)
    accounts = {r['account'] for r in rows}
    assert len(accounts) == 1
    account = next(iter(accounts))
    regs = [r for r in directory if r['codigo_entidad'] == account]
    assert regs and all(r['nombre_entidad'] == owner for r in regs)
    nits = {fiscal(r['nit_evidence']) for r in rows} | {fiscal(r['nit_entidad']) for r in regs}
    assert nits == {fiscal(corroboration['nit'])}
    matches = [s for s in sigep if fiscal(s['NIT']) in nits and s['NATURALEZA JURÍDICA'] == 'Alcaldía'
               and s['SUBORDEN'] in ('Municipal', 'Distrital') and s['NIVEL'] == 'Central'
               and normalize_text(s['MUNICIPIO']) == normalize_text(corroboration['municipality'])
               and normalize_text(s['DEPARTAMENTO']) == normalize_text(record['departamento'])]
    assert len(matches) == 1
    sig = matches[0]
    code = sig['DIVIPOLA DEPARTAMENTO'].zfill(2) + sig['DIVIPOLA MUNICIPIO'].zfill(3)
    territory = get_municipal_territories().by_code[code]
    assert normalize_text(territory.municipality) == normalize_text(corroboration['municipality'])
    assert normalize_text(corroboration['department']) == normalize_text(territory.department)
    assert all(normalize_text(r['departamento']) == normalize_text(territory.department) for r in regs)
    # The variant relationship is explicitly reviewed in the case evidence,
    # never applied to other fiscal siblings or to declared group membership.
    assert corroboration['physical_canonical'] == owner and corroboration['nature'] == 'Alcaldía'
    return dict(codes=[code], authority='MANUAL_SIGEP_AND_OFFICIAL_ENTITY_REGISTER', account=account,
                sources={ds: [owner] for ds in DATASETS}, sigep_row=sig,
                corroboration=corroboration,
                reason='Exact physical account/name in both SECOP datasets and current directory; '
                'SIGEP central municipal executive explicitly publishes municipality/DIVIPOLA; '
                'the additional official entity register corroborates legal nature, municipality and fiscal identity. '
                'No fiscal sibling, BuyerUnit or group is added.')


def adjudicate():
    before = read(OUT / 'evidence/before_aliases_db.json.gz')
    before_sources = read(OUT / 'evidence/before_buyer_source_names.json.gz')
    analysis = read(M0 / 'analysis.json.gz')
    directory = read(M0 / 'sources/directory.json.gz')['rows']
    sigep = read(M0 / 'sources/sigep.json.gz')
    validation = validate_cohort(analysis, directory, sigep)
    save('directory_location_validation.json', validation)
    live = verify_capture('directory_live.json')['rows']
    historical = verified_history()
    owners = ownership(before)
    pair = [r for r in before if r['official_name'] in (A, B)]
    aliases = sorted({a for r in pair for a in [r['official_name'], *r['aliases']]})
    third = {a: sorted(owners[normalize_text(a)] - {A, B}) for a in aliases
             if owners[normalize_text(a)] - {A, B}}
    assert len(pair) == 2 and not third
    assert not any(b['canonical_name'] in (A, B) or any(A in ns or B in ns for ns in b['sources'].values())
                   for b in before_sources['bindings'])
    recognized = next(r for r in analysis['recognized_units'] if r['canonical'] == B)
    assert recognized['account'] == ACCOUNT and recognized['municipality'] == '13836'
    assert not any(r['name'] == A for r in analysis['strata'])
    assert not any(r.get('nombre_entidad') == A for r in directory + live)
    regs = [r for r in live if r['codigo_entidad'] == ACCOUNT]
    assert len(regs) == 1 and regs[0]['nombre_entidad'] == B and regs[0]['nit_entidad'] == '890481149'
    inactive = [r for r in live if r['codigo_entidad'] == '706283421']
    assert len(inactive) == 1 and inactive[0]['esta_activa'] == 'No'
    other_accounts = [r for r in live if r['codigo_entidad'] in ('706283421', '708966437')]
    for ds in DATASETS:
        assert verify_capture(f'turbaco_other_accounts_{ds}.json')['rows'] == []
    manual = dict(canonical=A, surviving_canonical=B, expected_divipola='13836', account=ACCOUNT,
                  nit_evidence=['890481149'], physical_source_names=recognized['source_names'],
                  historical_physical_rows=historical, m0_recognized_unit=recognized,
                  m0_strata=[r for r in analysis['strata'] if r['account'] == ACCOUNT],
                  exact_synthetic_physical_rows=0, third_alias_owners=third, third_source_binding_owners=[],
                  inactive_account=inactive[0], inactive_account_historical_rows=0,
                  inactive_account_disposition='REGISTRY_ONLY_NO_OBSERVED_ACTIVITY; not merged, bound or reclassified as a BuyerUnit',
                  other_registered_accounts=other_accounts,
                  other_accounts_history={ds: [] for ds in DATASETS},
                  third_account_note='708966437 (ALCALDiA DE TURBACO 2, NIT 8904811490) is also registry-only '
                  'in the queried history. The trailing zero is NOT treated as a fiscal equivalence. '
                  'Both extra accounts remain excluded from identity and projection.',
                  prior_records=pair, shared_alias_keys=sorted({normalize_text(a) for a in aliases
                                                              if owners[normalize_text(a)] == {A, B}}),
                  evidence_sources=['turbaco_history_p6dx-8zbt.json', 'turbaco_history_contracts_retry.json',
                                    'directory_live.json', 'turbaco_other_accounts_p6dx-8zbt.json',
                                    'turbaco_other_accounts_jbjy-vk9h.json', 'M0 recognized_units and SIGEP 1213'],
                  git_origin=dict(synthetic_canonical='66e6242', physical_duplicate_appended='7781d6d'),
                  decision='DUPLICATE_BUYER_UNIT', confidence='HIGH_WITHIN_PUBLISHED_SECOP_HISTORY',
                  authority_type='MANUAL_EXACT_PHYSICAL_OWNERSHIP_ADJUDICATION', manual_authorization=True,
                  automatic_five_condition_predicate_applied=False, no_nit_merge=True,
                  reason='A is a synthetic catalog record with no exact published physical activity or directory row. '
                  'B is the uniquely observed executive account/name in both entire-history datasets, M0 and current directory. '
                  'A and B share human aliases; no third canonical claims them. Consolidate only these catalog records '
                  'into B, retain human input and explicitly bind B to exact source names (ADR-073). '
                  'The inactive same-NIT account is excluded, not merged. Token MUNICIPAL prevents the automatic '
                  'five-condition rule; this is the task-authorized manual adjudication, not a similarity rule.')
    save('evidence/turbaco_manual_identity.json', manual)
    cases = [manual]
    supplementary = read(OUT / 'evidence/official_register_corroboration.json')
    assert sha((OUT / 'evidence' / supplementary['file']).read_bytes()) == supplementary['sha256']
    for owner, code in EXPECTED.items():
        if owner == A:
            continue
        record = next(r for r in before if r['official_name'] == owner)
        rows = [r for r in analysis['strata'] if r['name'] == owner]
        authority = directory_affiliation(record, analysis['strata'], directory, validation)
        live_authority = directory_affiliation(record, analysis['strata'], live, validation)
        if authority['codes']:
            assert live_authority['codes'] == authority['codes']
        elif owner in supplementary['entities']:
            authority = official_sigep_affiliation(record, rows, live, sigep, supplementary['entities'][owner])
        assert not authority['codes'] or authority['codes'] == [code]
        relevant_sigep = [s for s in sigep if normalize_text(s['MUNICIPIO']) == normalize_text(
            get_municipal_territories().by_code[code].municipality) and s['NATURALEZA JURÍDICA'] == 'Alcaldía']
        conflicts = []
        for s in relevant_sigep:
            if fiscal(s['NIT']) not in {fiscal(r['nit_evidence']) for r in rows}:
                conflicts.append(dict(field='NIT', sigep=s['NIT'], secop=sorted({r['nit_evidence'] for r in rows}),
                                      disposition='Unresolved fiscal disagreement; no SIGEP-NIT merge or correction'))
            sig_code = s['DIVIPOLA DEPARTAMENTO'].zfill(2) + s['DIVIPOLA MUNICIPIO'].zfill(3)
            if sig_code != code:
                conflicts.append(dict(field='SIGEP_DIVIPOLA', sigep=sig_code, secop=code,
                                      in_municipal_registry=sig_code in get_municipal_territories().by_code,
                                      disposition='Invalid SIGEP code preserved; affiliation uses independently validated SECOP directory'))
        cases.append(dict(canonical=owner, expected_divipola=code,
                          account=next(iter({r['account'] for r in rows})),
                          nit_evidence=sorted({r['nit_evidence'] for r in rows}),
                          physical_source_names={ds: [owner] for ds in DATASETS}, m0_strata=rows,
                          m0_sigep_rows=relevant_sigep, source_conflicts=conflicts,
                          decision='VERIFIED_MUNICIPAL_AFFILIATION' if authority['codes'] else 'UNRESOLVED_REVIEW',
                          confidence='HIGH_SCOPED_AFFILIATION' if authority['codes'] else 'INSUFFICIENT',
                          authority_type=authority['authority'], affiliation=authority, reason=authority['reason'],
                          evidence_sources=['M0 analysis/strata', 'M0 directory', 'directory_live.json',
                                            'directory_location_validation.json' if authority['authority'].startswith('SECOP_')
                                            else 'official_register_corroboration.json; M0 SIGEP']))
    result = dict(status='PASS' if all(c['decision'] != 'UNRESOLVED_REVIEW' for c in cases) else 'PARTIAL',
                  cases=cases, production_promoted=0, no_nit_merge=True,
                  scope='Affiliation audit for five existing reviews only; no M0 amendments or 503 proposal promotion',
                  policy_unchanged_baseline_review_introduced=False)
    save('adjudication.json', result)
    return result


if __name__ == '__main__':
    result = adjudicate()
    print(result['status'], [(c['canonical'], c['decision']) for c in result['cases']], flush=True)
